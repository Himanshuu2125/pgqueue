package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
)

// jobCols reports the derived "scheduled" state; it is only ever read, never written.
const jobCols = `id, tenant_id, queue, type, payload, priority,
  CASE WHEN state = 'pending' AND run_at > now() THEN 'scheduled' ELSE state END,
  attempts, max_attempts, timeout_seconds, run_at, idempotency_key, lease_owner, lease_expires_at,
  lease_token, cancel_requested, last_error, result, cron_id, trace_parent,
  created_at, started_at, finished_at, updated_at`

func scanJob(row pgx.Row) (model.Job, error) {
	var j model.Job
	err := row.Scan(&j.ID, &j.TenantID, &j.Queue, &j.Type, &j.Payload, &j.Priority, &j.State,
		&j.Attempts, &j.MaxAttempts, &j.TimeoutSeconds, &j.RunAt, &j.IdempotencyKey, &j.LeaseOwner,
		&j.LeaseExpiresAt, &j.LeaseToken, &j.CancelRequested, &j.LastError, &j.Result, &j.CronID,
		&j.TraceParent, &j.CreatedAt, &j.StartedAt, &j.FinishedAt, &j.UpdatedAt)
	j.UTC()
	return j, err
}

func collectJobs(rows pgx.Rows, err error) ([]model.Job, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// nullJSON maps an empty raw message to SQL NULL.
func nullJSON(m json.RawMessage) any {
	if len(m) == 0 {
		return nil
	}
	return string(m)
}

// Enqueue inserts jobs in one transaction, honouring idempotency keys.
func (p *Postgres) Enqueue(ctx context.Context, jobs []NewJob) ([]EnqueueResult, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("enqueue begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	out, err := enqueueTx(ctx, tx, jobs)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("enqueue commit: %w", err)
	}
	return out, nil
}

func enqueueTx(ctx context.Context, tx pgx.Tx, jobs []NewJob) ([]EnqueueResult, error) {
	out := make([]EnqueueResult, 0, len(jobs))
	for _, n := range jobs {
		j, err := scanJob(tx.QueryRow(ctx, `
			INSERT INTO jobs (tenant_id, queue, type, payload, priority, state, max_attempts, timeout_seconds, run_at,
			  idempotency_key, cron_id, trace_parent) VALUES ($1, $2, $3, $4, $5, 'pending', $6, $7,
			  CASE WHEN $8::float8 IS NOT NULL THEN now() + make_interval(secs => $8::float8) WHEN $9::timestamptz IS NOT NULL
			  THEN GREATEST($9::timestamptz, now()) ELSE now() END, $10, $11, NULLIF($12, ''))
			ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING `+jobCols,
			n.TenantID, n.Queue, n.Type, string(n.Payload), n.Priority, n.MaxAttempts, n.TimeoutSeconds,
			n.DelaySeconds, n.RunAt, n.IdempotencyKey, n.CronID, n.TraceParent))
		created := err == nil
		if errors.Is(err, pgx.ErrNoRows) {
			j, err = scanJob(tx.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2`,
				n.TenantID, n.IdempotencyKey))
		}
		if err != nil {
			return nil, fmt.Errorf("enqueue insert: %w", err)
		}
		if created {
			if _, err := tx.Exec(ctx, `INSERT INTO job_events(job_id, event, attempt) VALUES ($1, 'submitted', 0)`, j.ID); err != nil {
				return nil, fmt.Errorf("enqueue event: %w", err)
			}
		}
		out = append(out, EnqueueResult{Job: j, Created: created})
	}
	return out, nil
}

// Get returns a job and its last EventsOnGet events, newest first.
func (p *Postgres) Get(ctx context.Context, tenant string, id uuid.UUID) (model.Job, []model.Event, error) {
	j, err := scanJob(p.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1 AND ($2 = '' OR tenant_id = $2)`, id, tenant))
	if errors.Is(err, pgx.ErrNoRows) {
		return j, nil, model.ErrNotFound
	} else if err != nil {
		return j, nil, fmt.Errorf("get job: %w", err)
	}
	rows, err := p.pool.Query(ctx, `SELECT event, attempt, worker_id, detail, created_at FROM job_events
		WHERE job_id = $1 ORDER BY id DESC LIMIT $2`, id, config.EventsOnGet)
	if err != nil {
		return j, nil, fmt.Errorf("get events: %w", err)
	}
	events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (model.Event, error) {
		var e model.Event
		err := r.Scan(&e.Event, &e.Attempt, &e.WorkerID, &e.Detail, &e.CreatedAt)
		e.CreatedAt = e.CreatedAt.UTC()
		return e, err
	})
	if err != nil {
		return j, nil, fmt.Errorf("get events: %w", err)
	}
	return j, events, nil
}

// List pages through jobs ordered by created_at DESC, id DESC.
func (p *Postgres) List(ctx context.Context, f Filter) ([]model.Job, string, error) {
	q, args := `SELECT `+jobCols+` FROM jobs WHERE true`, []any{}
	add := func(col string, v any) {
		args = append(args, v)
		q += fmt.Sprintf(" AND %s = $%d", col, len(args))
	}
	if f.TenantID != "" {
		add("tenant_id", f.TenantID)
	}
	switch f.State {
	case "":
	case model.StateScheduled:
		q += " AND state = 'pending' AND run_at > now()"
	case model.StatePending:
		q += " AND state = 'pending' AND run_at <= now()"
	default:
		add("state", f.State)
	}
	if f.Queue != "" {
		add("queue", f.Queue)
	}
	if f.Type != "" {
		add("type", f.Type)
	}
	if f.Cursor != "" {
		ts, id, err := DecodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, ts, id)
		q += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	limit := f.Limit
	if limit <= 0 || limit > config.ListMax {
		limit = config.ListDefault
	}
	q += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT %d", limit+1)
	jobs, err := collectJobs(p.pool.Query(ctx, q, args...))
	if err != nil {
		return nil, "", fmt.Errorf("list jobs: %w", err)
	}
	next := ""
	if len(jobs) > limit {
		jobs = jobs[:limit]
		last := jobs[limit-1]
		next = EncodeCursor(last.CreatedAt, last.ID)
	}
	return jobs, next, nil
}

// Claim leases up to n runnable jobs.
func (p *Postgres) Claim(ctx context.Context, worker string, queues []string, n int, lease time.Duration) ([]model.Job, error) {
	jobs, err := collectJobs(p.pool.Query(ctx, `
		WITH picked AS (
		  SELECT id FROM jobs WHERE state = 'pending' AND run_at <= now() AND queue = ANY($1)
		    AND queue NOT IN (SELECT name FROM queues WHERE paused)
		  ORDER BY priority DESC, run_at FOR UPDATE SKIP LOCKED LIMIT $2),
		upd AS (
		  UPDATE jobs j SET state = 'running', attempts = j.attempts + 1, lease_owner = $3,
		    lease_expires_at = now() + make_interval(secs => $4), lease_token = nextval('lease_seq'),
		    started_at = now(), updated_at = now()
		  FROM picked WHERE j.id = picked.id RETURNING j.*),
		ev AS (INSERT INTO job_events(job_id, event, attempt, worker_id) SELECT id, 'claimed', attempts, $3 FROM upd)
		SELECT `+jobCols+` FROM upd`, queues, n, worker, lease.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return jobs, nil
}

func leaseArrays(ls []Lease) ([]string, []int64) {
	ids, toks := make([]string, len(ls)), make([]int64, len(ls))
	for i, l := range ls {
		ids[i], toks[i] = l.JobID.String(), l.Token
	}
	return ids, toks
}

// Heartbeat extends all leases in one statement and reports lost and cancel-requested ids.
func (p *Postgres) Heartbeat(ctx context.Context, leases []Lease, lease time.Duration) (lost, cancel []uuid.UUID, err error) {
	ids, toks := leaseArrays(leases)
	rows, err := p.pool.Query(ctx, `
		UPDATE jobs j SET lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		FROM unnest($1::uuid[], $2::bigint[]) AS l(id, tok)
		WHERE j.id = l.id AND j.lease_token = l.tok AND j.state = 'running'
		RETURNING j.id, j.cancel_requested`, ids, toks, lease.Seconds())
	if err != nil {
		return nil, nil, fmt.Errorf("heartbeat: %w", err)
	}
	defer rows.Close()
	alive := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		var cr bool
		if err := rows.Scan(&id, &cr); err != nil {
			return nil, nil, fmt.Errorf("heartbeat scan: %w", err)
		}
		alive[id] = true
		if cr {
			cancel = append(cancel, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("heartbeat rows: %w", err)
	}
	for _, l := range leases {
		if !alive[l.JobID] {
			lost = append(lost, l.JobID)
		}
	}
	return lost, cancel, nil
}

// fenced runs an ack statement and maps 0 affected rows to ErrLeaseLost.
func (p *Postgres) fenced(ctx context.Context, sql string, args ...any) error {
	tag, err := p.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	if tag.RowsAffected() == 0 {
		observability.StaleAcks.Inc()
		return model.ErrLeaseLost
	}
	return nil
}

// Complete marks a job succeeded. Success wins over a pending cancel.
func (p *Postgres) Complete(ctx context.Context, l Lease, result json.RawMessage) error {
	return p.fenced(ctx, `
		WITH u AS (UPDATE jobs SET state = 'succeeded', result = $3, finished_at = now(), lease_owner = NULL,
		  lease_expires_at = NULL, updated_at = now() WHERE id = $1 AND lease_token = $2 AND state = 'running' RETURNING id, attempts)
		INSERT INTO job_events(job_id, event, attempt) SELECT id, 'succeeded', attempts FROM u`,
		l.JobID, l.Token, nullJSON(result))
}

// Fail retries the job after retryDelay, or makes it dead (nil delay) or cancelled (cancel requested).
func (p *Postgres) Fail(ctx context.Context, l Lease, errMsg string, retryDelay *time.Duration) error {
	var delayMS *int64
	if retryDelay != nil {
		ms := retryDelay.Milliseconds()
		delayMS = &ms
	}
	return p.fenced(ctx, `
		WITH u AS (UPDATE jobs SET
		    state = CASE WHEN cancel_requested THEN 'cancelled' WHEN $4::bigint IS NULL THEN 'dead' ELSE 'pending' END,
		    run_at = CASE WHEN NOT cancel_requested AND $4::bigint IS NOT NULL THEN now() + $4::bigint * interval '1 millisecond' ELSE run_at END,
		    last_error = $3, lease_owner = NULL, lease_expires_at = NULL, updated_at = now(),
		    finished_at = CASE WHEN cancel_requested OR $4::bigint IS NULL THEN now() END
		  WHERE id = $1 AND lease_token = $2 AND state = 'running' RETURNING id, state, attempts)
		INSERT INTO job_events(job_id, event, attempt, detail)
		SELECT id, CASE state WHEN 'pending' THEN 'retried' ELSE state END, attempts,
		  CASE state WHEN 'pending' THEN jsonb_build_object('error', $3::text, 'retry_in_ms', $4::bigint)
		             WHEN 'dead' THEN jsonb_build_object('error', $3::text) END
		FROM u`, l.JobID, l.Token, clean(errMsg, config.LastErrorCap), delayMS)
}

// Release returns interrupted jobs to pending on shutdown, undoing the claim's attempt.
func (p *Postgres) Release(ctx context.Context, ls []Lease) error {
	if len(ls) == 0 {
		return nil
	}
	ids, toks := leaseArrays(ls)
	_, err := p.pool.Exec(ctx, `
		WITH u AS (UPDATE jobs SET state = CASE WHEN cancel_requested THEN 'cancelled' ELSE 'pending' END,
		    attempts = GREATEST(attempts - 1, 0), run_at = now(), finished_at = CASE WHEN cancel_requested THEN now() END,
		    lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
		  WHERE (id, lease_token) IN (SELECT * FROM unnest($1::uuid[], $2::bigint[])) AND state = 'running'
		  RETURNING id, state, attempts)
		INSERT INTO job_events(job_id, event, attempt)
		SELECT id, CASE state WHEN 'cancelled' THEN 'cancelled' ELSE 'released' END, attempts FROM u`, ids, toks)
	if err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return nil
}

// transition locks a tenant-scoped job, lets decide pick the update, applies it with its event.
func (p *Postgres) transition(ctx context.Context, tenant string, id uuid.UUID,
	decide func(state string) (set, event string, err error)) (model.Job, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return model.Job{}, fmt.Errorf("transition begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var state, queue, typ string
	err = tx.QueryRow(ctx, `SELECT state, queue, type FROM jobs WHERE id = $1 AND ($2 = '' OR tenant_id = $2) FOR UPDATE`,
		id, tenant).Scan(&state, &queue, &typ)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Job{}, model.ErrNotFound
	} else if err != nil {
		return model.Job{}, fmt.Errorf("transition lock: %w", err)
	}
	set, event, err := decide(state)
	if err != nil {
		return model.Job{}, err
	}
	if set != "" {
		if _, err := tx.Exec(ctx, `WITH u AS (UPDATE jobs SET `+set+`, updated_at = now() WHERE id = $1 RETURNING id, attempts)
			INSERT INTO job_events(job_id, event, attempt) SELECT id, $2, attempts FROM u`, id, event); err != nil {
			return model.Job{}, fmt.Errorf("transition update: %w", err)
		}
		if event == model.StateCancelled {
			observability.JobsCompleted.WithLabelValues(queue, typ, model.StateCancelled).Inc()
		}
	}
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1`, id))
	if err != nil {
		return j, fmt.Errorf("transition reload: %w", err)
	}
	return j, tx.Commit(ctx)
}

// Cancel cancels a pending job, or requests cancellation of a running one.
func (p *Postgres) Cancel(ctx context.Context, tenant string, id uuid.UUID) (model.Job, error) {
	return p.transition(ctx, tenant, id, func(state string) (string, string, error) {
		switch state {
		case model.StatePending:
			return `state = 'cancelled', finished_at = now()`, "cancelled", nil
		case model.StateRunning:
			return `cancel_requested = true`, "cancel_requested", nil
		case model.StateCancelled:
			return "", "", nil
		}
		return "", "", model.ErrInvalidState
	})
}

// Replay moves a dead job back to pending with attempts reset.
func (p *Postgres) Replay(ctx context.Context, tenant string, id uuid.UUID) (model.Job, error) {
	return p.transition(ctx, tenant, id, func(state string) (string, string, error) {
		if state != model.StateDead {
			return "", "", model.ErrInvalidState
		}
		return `state = 'pending', attempts = 0, run_at = now(), cancel_requested = false, result = NULL,
			finished_at = NULL, lease_owner = NULL, lease_expires_at = NULL`, "replayed", nil
	})
}
