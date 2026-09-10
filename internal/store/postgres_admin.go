package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/robfig/cron/v3"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/model"
)

// SeedTenants upserts every SEED_TENANTS entry.
func (p *Postgres) SeedTenants(ctx context.Context, ts []config.Tenant) error {
	for _, t := range ts {
		if _, err := p.pool.Exec(ctx, `
			INSERT INTO tenants(id, api_key_hash, rate_limit_per_sec, burst) VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE SET api_key_hash=EXCLUDED.api_key_hash,
			  rate_limit_per_sec=EXCLUDED.rate_limit_per_sec, burst=EXCLUDED.burst`,
			t.ID, HashKey(t.Key), t.Rate, t.Burst); err != nil {
			return fmt.Errorf("seed tenant %s: %w", t.ID, err)
		}
	}
	return nil
}

// TenantByKey looks a tenant up by the hash of its API key.
func (p *Postgres) TenantByKey(ctx context.Context, key string) (Tenant, error) {
	var t Tenant
	err := p.pool.QueryRow(ctx, `SELECT id, rate_limit_per_sec, burst FROM tenants WHERE api_key_hash = $1`,
		HashKey(key)).Scan(&t.ID, &t.Rate, &t.Burst)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, model.ErrNotFound
	} else if err != nil {
		return t, fmt.Errorf("tenant lookup: %w", err)
	}
	return t, nil
}

// ParseCron parses a 5-field or descriptor expression, rejecting time zones and sub-minute @every.
func ParseCron(expr string) (cron.Schedule, error) {
	e := strings.TrimSpace(expr)
	if strings.HasPrefix(e, "CRON_TZ=") || strings.HasPrefix(e, "TZ=") {
		return nil, errors.New("time zone prefixes are not allowed; schedules run in UTC")
	}
	s, err := cron.ParseStandard(e)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}
	if every, ok := s.(cron.ConstantDelaySchedule); ok && every.Delay < time.Minute {
		return nil, errors.New("@every interval must be at least 1m")
	}
	return s, nil
}

// NextRun evaluates s in UTC.
func NextRun(s cron.Schedule, now time.Time) time.Time { return s.Next(now.UTC()) }

const cronCols = `id, tenant_id, name, expr, queue, type, payload, enabled, last_run_at, next_run_at, created_at`

func scanCron(row pgx.Row) (Cron, error) {
	var c Cron
	err := row.Scan(&c.ID, &c.TenantID, &c.Name, &c.Expr, &c.Queue, &c.Type, &c.Payload, &c.Enabled,
		&c.LastRunAt, &c.NextRunAt, &c.CreatedAt)
	if c.LastRunAt != nil {
		*c.LastRunAt = c.LastRunAt.UTC()
	}
	c.NextRunAt, c.CreatedAt = c.NextRunAt.UTC(), c.CreatedAt.UTC()
	if errors.Is(err, pgx.ErrNoRows) {
		return c, model.ErrNotFound
	}
	return c, err
}

func (p *Postgres) dbNow(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (time.Time, error) {
	var now time.Time
	if err := q.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return now, fmt.Errorf("db now: %w", err)
	}
	return now, nil
}

// CreateCron inserts a schedule whose first run is Next(db now).
func (p *Postgres) CreateCron(ctx context.Context, c Cron) (Cron, error) {
	s, err := ParseCron(c.Expr)
	if err != nil {
		return c, err
	}
	now, err := p.dbNow(ctx, p.pool)
	if err != nil {
		return c, err
	}
	out, err := scanCron(p.pool.QueryRow(ctx, `INSERT INTO cron_schedules(tenant_id, name, expr, queue, type, payload, enabled, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING `+cronCols,
		c.TenantID, c.Name, c.Expr, c.Queue, c.Type, string(c.Payload), c.Enabled, NextRun(s, now)))
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return out, model.ErrDuplicateName
	} else if err != nil {
		return out, fmt.Errorf("create cron: %w", err)
	}
	return out, nil
}

// ListCron returns up to 100 schedules of a tenant.
func (p *Postgres) ListCron(ctx context.Context, tenant string) ([]Cron, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+cronCols+` FROM cron_schedules WHERE tenant_id = $1 ORDER BY created_at LIMIT 100`, tenant)
	if err != nil {
		return nil, fmt.Errorf("list cron: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Cron, error) { return scanCron(r) })
	if err != nil {
		return nil, fmt.Errorf("list cron: %w", err)
	}
	return out, nil
}

// SetCronEnabled toggles a schedule; re-enabling sets next_run_at = Next(now()).
func (p *Postgres) SetCronEnabled(ctx context.Context, tenant string, id uuid.UUID, enabled bool) (Cron, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Cron{}, fmt.Errorf("cron begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	c, err := scanCron(tx.QueryRow(ctx, `SELECT `+cronCols+` FROM cron_schedules WHERE id = $1 AND tenant_id = $2 FOR UPDATE`, id, tenant))
	if err != nil {
		return c, err
	}
	next := c.NextRunAt
	if enabled {
		s, err := ParseCron(c.Expr)
		if err != nil {
			return c, err
		}
		now, err := p.dbNow(ctx, tx)
		if err != nil {
			return c, err
		}
		next = NextRun(s, now)
	}
	c, err = scanCron(tx.QueryRow(ctx, `UPDATE cron_schedules SET enabled = $2, next_run_at = $3 WHERE id = $1 RETURNING `+cronCols,
		id, enabled, next))
	if err != nil {
		return c, fmt.Errorf("cron update: %w", err)
	}
	return c, tx.Commit(ctx)
}

// DeleteCron removes a schedule; its jobs keep running with cron_id NULL.
func (p *Postgres) DeleteCron(ctx context.Context, tenant string, id uuid.UUID) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM cron_schedules WHERE id = $1 AND tenant_id = $2`, id, tenant)
	if err != nil {
		return fmt.Errorf("delete cron: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return model.ErrNotFound
	}
	return nil
}

// FireDueCron enqueues one job per due schedule, each in its own transaction.
func (p *Postgres) FireDueCron(ctx context.Context) (int, error) {
	fired := 0
	for fired < 100 {
		ok, err := p.fireOne(ctx)
		if err != nil || !ok {
			return fired, err
		}
		fired++
	}
	return fired, nil
}

func (p *Postgres) fireOne(ctx context.Context) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("cron begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	now, err := p.dbNow(ctx, tx)
	if err != nil {
		return false, err
	}
	c, err := scanCron(tx.QueryRow(ctx, `SELECT `+cronCols+` FROM cron_schedules WHERE enabled AND next_run_at <= $1
		ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT 1`, now))
	if errors.Is(err, model.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("cron select: %w", err)
	}
	s, err := ParseCron(c.Expr)
	if err != nil {
		return false, fmt.Errorf("cron %s: %w", c.ID, err)
	}
	key := fmt.Sprintf("cron:%s:%d", c.ID, c.NextRunAt.Unix())
	if _, err := enqueueTx(ctx, tx, []NewJob{{TenantID: c.TenantID, Queue: c.Queue, Type: c.Type, Payload: c.Payload,
		MaxAttempts: p.defaultMaxAttempts, TimeoutSeconds: p.defaultTimeout, IdempotencyKey: &key, CronID: &c.ID}}); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE cron_schedules SET last_run_at = $2, next_run_at = $3 WHERE id = $1`,
		c.ID, now, NextRun(s, now)); err != nil {
		return false, fmt.Errorf("cron advance: %w", err)
	}
	return true, tx.Commit(ctx)
}

// SetQueuePaused pauses or resumes claiming from a queue.
func (p *Postgres) SetQueuePaused(ctx context.Context, name string, paused bool) error {
	tag, err := p.pool.Exec(ctx, `UPDATE queues SET paused = $2 WHERE name = $1`, name, paused)
	if err != nil {
		return fmt.Errorf("pause queue: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return model.ErrNotFound
	}
	return nil
}

// ListQueues returns every queue ordered by name.
func (p *Postgres) ListQueues(ctx context.Context) ([]Queue, error) {
	rows, err := p.pool.Query(ctx, `SELECT name, paused FROM queues ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	qs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Queue])
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	return qs, nil
}

// ReapExpired reclaims expired leases in batches of ReaperBatch.
func (p *Postgres) ReapExpired(ctx context.Context) (int, error) {
	total := 0
	for {
		var n int
		err := p.pool.QueryRow(ctx, `
			WITH exp AS (SELECT id, lease_expires_at AS old_exp FROM jobs WHERE state = 'running' AND lease_expires_at < now()
			  ORDER BY lease_expires_at LIMIT 500 FOR UPDATE SKIP LOCKED),
			upd AS (UPDATE jobs j SET
			    state = CASE WHEN j.cancel_requested THEN 'cancelled' WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'pending' END,
			    run_at = now() + (random() * LEAST($2::float8, $1::float8 * power(2, j.attempts - 1))) * interval '1 millisecond',
			    last_error = 'lease_expired', lease_owner = NULL, lease_expires_at = NULL, updated_at = now(),
			    finished_at = CASE WHEN j.cancel_requested OR j.attempts >= j.max_attempts THEN now() END
			  FROM exp WHERE j.id = exp.id RETURNING j.id, j.state, j.attempts, j.lease_token, exp.old_exp),
			ev AS (INSERT INTO job_events(job_id, event, attempt, detail) SELECT id, 'reclaimed', attempts,
			  jsonb_build_object('expired_at', old_exp, 'expired_lease_token', lease_token) FROM upd)
			SELECT count(*) FROM upd`, p.backoffBase.Milliseconds(), p.backoffMax.Milliseconds()).Scan(&n)
		if err != nil {
			return total, fmt.Errorf("reap: %w", err)
		}
		total += n
		if n < config.ReaperBatch {
			return total, nil
		}
	}
}

const reportedState = `CASE WHEN state = 'pending' AND run_at > now() THEN 'scheduled' ELSE state END`

// Stats counts jobs by queue and reported state; tenant "" means all tenants.
func (p *Postgres) Stats(ctx context.Context, tenant string) (Stats, error) {
	qs, err := p.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	out := Stats{}
	for _, q := range qs {
		out[q.Name] = map[string]int64{}
		for _, s := range model.States {
			out[q.Name][s] = 0
		}
	}
	rows, err := p.pool.Query(ctx, `SELECT queue, `+reportedState+`, count(*) FROM jobs
		WHERE $1 = '' OR tenant_id = $1 GROUP BY 1, 2`, tenant)
	if err != nil {
		return nil, fmt.Errorf("stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var q, s string
		var n int64
		if err := rows.Scan(&q, &s, &n); err != nil {
			return nil, fmt.Errorf("stats scan: %w", err)
		}
		out[q][s] = n
	}
	return out, rows.Err()
}

// Depth samples the queue_depth and oldest-pending gauges in one GROUP BY query.
func (p *Postgres) Depth(ctx context.Context) ([]DepthRow, error) {
	rows, err := p.pool.Query(ctx, `SELECT queue, `+reportedState+`, count(*),
		  COALESCE(EXTRACT(EPOCH FROM now() - min(run_at) FILTER (WHERE state = 'pending' AND run_at <= now())), 0)::float8
		FROM jobs WHERE state IN ('pending', 'running', 'dead') GROUP BY 1, 2`)
	if err != nil {
		return nil, fmt.Errorf("depth: %w", err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[DepthRow])
	if err != nil {
		return nil, fmt.Errorf("depth: %w", err)
	}
	return out, nil
}

// LeaderID returns the application_name (worker id) holding the leader lock, or "".
func (p *Postgres) LeaderID(ctx context.Context) (string, error) {
	var id string
	err := p.pool.QueryRow(ctx, `SELECT a.application_name FROM pg_locks l JOIN pg_stat_activity a USING (pid)
		WHERE l.locktype = 'advisory' AND l.classid = 0 AND l.objid = $1 AND l.granted`, config.LeaderLockID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("leader id: %w", err)
	}
	return id, nil
}
