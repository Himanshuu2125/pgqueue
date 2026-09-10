// Package store is the Postgres persistence layer.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/model"
)

// Lease identifies one claim of a job; the token fences stale writers.
type Lease struct {
	JobID uuid.UUID
	Token int64
}

// NewJob is the input to Enqueue.
type NewJob struct {
	TenantID, Queue, Type                 string
	Payload                               json.RawMessage
	Priority, MaxAttempts, TimeoutSeconds int
	RunAt                                 *time.Time
	DelaySeconds                          *int
	IdempotencyKey                        *string
	CronID                                *uuid.UUID
	TraceParent                           string
}

// EnqueueResult is one Enqueue outcome; Created is false for an idempotent hit.
type EnqueueResult struct {
	Job     model.Job
	Created bool
}

// Filter selects jobs for List.
type Filter struct {
	TenantID           string // "" = all tenants (admin only)
	State, Queue, Type string // State may be "scheduled"
	Limit              int
	Cursor             string
}

// Cron is one cron_schedules row.
type Cron struct {
	ID        uuid.UUID       `json:"id"`
	TenantID  string          `json:"-"`
	Name      string          `json:"name"`
	Expr      string          `json:"expr"`
	Queue     string          `json:"queue"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Enabled   bool            `json:"enabled"`
	LastRunAt *time.Time      `json:"last_run_at"`
	NextRunAt time.Time       `json:"next_run_at"`
	CreatedAt time.Time       `json:"created_at"`
}

// Queue is one queues row.
type Queue struct {
	Name   string
	Paused bool
}

// Stats maps queue -> reported state -> count.
type Stats map[string]map[string]int64

// DepthRow is one queue/state gauge sample.
type DepthRow struct {
	Queue, State string
	Count        int64
	OldestAge    float64
}

// Tenant is an authenticated tenant.
type Tenant struct {
	ID          string
	Rate, Burst int
}

// WorkerStore is the only interface; faked in worker unit tests.
type WorkerStore interface {
	Claim(ctx context.Context, worker string, queues []string, n int, lease time.Duration) ([]model.Job, error)
	Heartbeat(ctx context.Context, leases []Lease, lease time.Duration) (lost, cancel []uuid.UUID, err error)
	Complete(ctx context.Context, l Lease, result json.RawMessage) error               // ErrLeaseLost if fenced
	Fail(ctx context.Context, l Lease, errMsg string, retryDelay *time.Duration) error // nil → dead/cancelled
	Release(ctx context.Context, ls []Lease) error
}

// Postgres implements WorkerStore and the api/scheduler methods.
type Postgres struct {
	pool                               *pgxpool.Pool
	backoffBase, backoffMax            time.Duration
	defaultMaxAttempts, defaultTimeout int
}

// New wraps a pool. Backoff bounds feed the reaper; defaults feed cron jobs.
func New(pool *pgxpool.Pool, c config.Config) *Postgres {
	return &Postgres{pool: pool, backoffBase: c.BackoffBase, backoffMax: c.BackoffMax,
		defaultMaxAttempts: c.DefaultMaxAttempts, defaultTimeout: c.DefaultTimeoutSeconds}
}

// Pool exposes the underlying pool (readiness checks).
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// HashKey is the API key hash used by seeding and auth lookup.
func HashKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// EncodeCursor encodes a list position.
func EncodeCursor(createdAt time.Time, id uuid.UUID) string {
	return base64.URLEncoding.EncodeToString([]byte(createdAt.UTC().Format(time.RFC3339Nano) + "|" + id.String()))
}

// DecodeCursor parses a cursor produced by EncodeCursor.
func DecodeCursor(c string) (time.Time, uuid.UUID, error) {
	b, err := base64.URLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, uuid.Nil, errors.New("bad cursor")
	}
	ts, id, ok := strings.Cut(string(b), "|")
	t, err1 := time.Parse(time.RFC3339Nano, ts)
	u, err2 := uuid.Parse(id)
	if !ok || err1 != nil || err2 != nil {
		return time.Time{}, uuid.Nil, errors.New("bad cursor")
	}
	return t, u, nil
}

// clean makes s storable as Postgres text: valid UTF-8, no NUL, at most n bytes.
func clean(s string, n int) string {
	s = strings.ReplaceAll(strings.ToValidUTF8(s, "�"), "\x00", "")
	if len(s) > n {
		s = s[:n]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}
