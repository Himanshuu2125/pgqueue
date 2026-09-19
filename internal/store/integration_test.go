package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/handlers"
	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
	"github.com/Himanshuu2125/pgqueue/internal/scheduler"
	"github.com/Himanshuu2125/pgqueue/internal/store"
	"github.com/Himanshuu2125/pgqueue/internal/worker"
)

var testCfg = config.Config{BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
	DefaultMaxAttempts: 5, DefaultTimeoutSeconds: 60}

func submit(t *testing.T, st *store.Postgres, n store.NewJob) model.Job {
	t.Helper()
	if n.MaxAttempts == 0 {
		n.MaxAttempts = 5
	}
	if n.TimeoutSeconds == 0 {
		n.TimeoutSeconds = 60
	}
	if n.Payload == nil {
		n.Payload = json.RawMessage(`{}`)
	}
	res, err := st.Enqueue(context.Background(), []store.NewJob{n})
	require.NoError(t, err)
	return res[0].Job
}

func get(t *testing.T, st *store.Postgres, id uuid.UUID) (model.Job, []string) {
	t.Helper()
	j, events, err := st.Get(context.Background(), "", id)
	require.NoError(t, err)
	var names []string
	for _, e := range events {
		names = append(names, e.Event)
	}
	return j, names
}

// runPool starts a worker pool for queue with handler run; the returned func stops it and waits.
func runPool(t *testing.T, st *store.Postgres, queue string, run handlers.Handler, mod func(*worker.Config)) func() {
	t.Helper()
	cfg := worker.Config{WorkerID: "test-" + randHex(4), Queues: []string{queue}, Concurrency: 4,
		Poll: 20 * time.Millisecond, Lease: 30 * time.Second, Heartbeat: 100 * time.Millisecond,
		ShutdownGrace: 5 * time.Second, BackoffBase: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond}
	if mod != nil {
		mod(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.New(cfg, st, run).Run(ctx) }()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			require.NoError(t, <-done)
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitState(t *testing.T, st *store.Postgres, id uuid.UUID, state string, within time.Duration) model.Job {
	t.Helper()
	var j model.Job
	require.Eventually(t, func() bool { j, _ = get(t, st, id); return j.State == state }, within, 20*time.Millisecond,
		"job never reached %s", state)
	return j
}

func registry(pool *pgxpool.Pool) *handlers.Registry {
	return handlers.NewRegistry(pool, nil, config.Handlers{})
}

func TestNoDoubleClaim(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	tenant, queue := fixture(t, pool)
	jobs := make([]store.NewJob, 1000)
	for i := range jobs {
		jobs[i] = store.NewJob{TenantID: tenant, Queue: queue, Type: "noop", Payload: json.RawMessage(`{}`), MaxAttempts: 5, TimeoutSeconds: 60}
	}
	_, err := st.Enqueue(context.Background(), jobs)
	require.NoError(t, err)
	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := st.Claim(context.Background(), "w", []string{queue}, 10, time.Minute)
				if err != nil || len(got) == 0 {
					return
				}
				mu.Lock()
				for _, j := range got {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Len(t, seen, 1000)
	for id, n := range seen {
		require.Equal(t, 1, n, "job %s claimed %d times", id, n)
	}
}

func TestLeaseExpiryReclaimAndRetry(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	tenant, queue := fixture(t, pool)
	j := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "noop"})
	got, err := st.Claim(context.Background(), "w1", []string{queue}, 1, 2*time.Second)
	require.NoError(t, err)
	require.Len(t, got, 1)
	time.Sleep(2200 * time.Millisecond) // no heartbeat: the lease expires
	n, err := st.ReapExpired(context.Background())
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)
	after, events := get(t, st, j.ID)
	assert.Contains(t, []string{model.StatePending, model.StateScheduled}, after.State)
	assert.Equal(t, "lease_expired", *after.LastError)
	assert.Equal(t, "reclaimed", events[0])
	require.Eventually(t, func() bool {
		got, _ = st.Claim(context.Background(), "w2", []string{queue}, 1, time.Minute)
		return len(got) == 1
	}, 2*time.Second, 20*time.Millisecond)
	assert.Equal(t, 2, got[0].Attempts)
}

func TestStaleAckAfterReap(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	tenant, queue := fixture(t, pool)
	submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "noop"})
	got, err := st.Claim(context.Background(), "w1", []string{queue}, 1, time.Second)
	require.NoError(t, err)
	time.Sleep(1200 * time.Millisecond)
	_, err = st.ReapExpired(context.Background())
	require.NoError(t, err)
	before := testutil.ToFloat64(observability.StaleAcks)
	err = st.Complete(context.Background(), store.Lease{JobID: got[0].ID, Token: *got[0].LeaseToken}, nil)
	require.ErrorIs(t, err, model.ErrLeaseLost)
	assert.Equal(t, before+1, testutil.ToFloat64(observability.StaleAcks))
}

func TestRetryDeadReplaySucceeded(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	tenant, queue := fixture(t, pool)
	var calls atomic.Int32
	runPool(t, st, queue, func(ctx context.Context, job model.Job) (json.RawMessage, error) {
		if calls.Add(1) <= 3 {
			return nil, errors.New("boom")
		}
		return json.RawMessage(`{"ok":true}`), nil
	}, nil)
	j := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "noop", MaxAttempts: 3})
	dead := waitState(t, st, j.ID, model.StateDead, 5*time.Second)
	assert.Equal(t, 3, dead.Attempts)
	assert.Equal(t, "boom", *dead.LastError)
	replayed, err := st.Replay(context.Background(), tenant, j.ID)
	require.NoError(t, err)
	assert.Equal(t, 0, replayed.Attempts)
	done := waitState(t, st, j.ID, model.StateSucceeded, 5*time.Second)
	assert.JSONEq(t, `{"ok":true}`, string(done.Result))
	_, events := get(t, st, j.ID)
	assert.Equal(t, []string{"succeeded", "claimed", "replayed", "dead", "claimed", "retried", "claimed", "retried", "claimed", "submitted"}, events)
}

func TestCancel(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	ctx := context.Background()
	tenant, queue := fixture(t, pool)

	pending := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "noop", DelaySeconds: ptr(3600)})
	c, err := st.Cancel(ctx, tenant, pending.ID)
	require.NoError(t, err)
	assert.Equal(t, model.StateCancelled, c.State)
	_, err = st.Cancel(ctx, tenant, pending.ID)
	require.NoError(t, err, "cancelling a cancelled job is a no-op")

	var observed atomic.Bool
	runPool(t, st, queue, func(ctx context.Context, job model.Job) (json.RawMessage, error) {
		if string(job.Payload) == `{"ignore": true}` {
			time.Sleep(500 * time.Millisecond) // ignores cancellation and succeeds
			return nil, nil
		}
		<-ctx.Done()
		observed.Store(true)
		return nil, ctx.Err()
	}, nil)
	running := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "noop"})
	waitState(t, st, running.ID, model.StateRunning, 2*time.Second)
	c, err = st.Cancel(ctx, tenant, running.ID)
	require.NoError(t, err)
	assert.True(t, c.CancelRequested)
	waitState(t, st, running.ID, model.StateCancelled, 3*time.Second)
	assert.True(t, observed.Load(), "handler observed cancellation")

	wins := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "noop", Payload: json.RawMessage(`{"ignore": true}`)})
	waitState(t, st, wins.ID, model.StateRunning, 2*time.Second)
	_, err = st.Cancel(ctx, tenant, wins.ID)
	require.NoError(t, err)
	waitState(t, st, wins.ID, model.StateSucceeded, 3*time.Second)
	_, err = st.Cancel(ctx, tenant, wins.ID)
	require.ErrorIs(t, err, model.ErrInvalidState)
}

func ptr[T any](v T) *T { return &v }

func TestTimeout(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	tenant, queue := fixture(t, pool)
	runPool(t, st, queue, registry(pool).Run, nil)
	j := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "sleep", Payload: json.RawMessage(`{"ms":5000}`),
		TimeoutSeconds: 1, MaxAttempts: 1})
	dead := waitState(t, st, j.ID, model.StateDead, 4*time.Second)
	assert.Equal(t, "timeout", *dead.LastError)
}

func TestShutdownReleasesJob(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	tenant, queue := fixture(t, pool)
	stop := runPool(t, st, queue, registry(pool).Run, func(c *worker.Config) { c.ShutdownGrace = 300 * time.Millisecond })
	j := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "sleep", Payload: json.RawMessage(`{"ms":60000}`)})
	waitState(t, st, j.ID, model.StateRunning, 2*time.Second)
	stop() // SIGTERM equivalent
	after, events := get(t, st, j.ID)
	assert.Equal(t, model.StatePending, after.State)
	assert.Equal(t, 0, after.Attempts)
	assert.Nil(t, after.LeaseOwner)
	assert.Equal(t, "released", events[0])
}

func TestPausedQueue(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	ctx := context.Background()
	tenant, queue := fixture(t, pool)
	require.NoError(t, st.SetQueuePaused(ctx, queue, true))
	runPool(t, st, queue, registry(pool).Run, nil)
	j := submit(t, st, store.NewJob{TenantID: tenant, Queue: queue, Type: "noop"})
	time.Sleep(300 * time.Millisecond)
	still, _ := get(t, st, j.ID)
	assert.Equal(t, model.StatePending, still.State)
	require.NoError(t, st.SetQueuePaused(ctx, queue, false))
	waitState(t, st, j.ID, model.StateSucceeded, 2*time.Second)
}

func startLeaders(t *testing.T, st *store.Postgres, n int) []*scheduler.Leader {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	ls := make([]*scheduler.Leader, n)
	for i := range ls {
		ls[i] = scheduler.NewLeader(dsn, "sched-"+randHex(4), st, time.Second)
		wg.Add(1)
		go func(l *scheduler.Leader) { defer wg.Done(); l.Run(ctx) }(ls[i])
	}
	return ls
}

func leaders(ls []*scheduler.Leader) []*scheduler.Leader {
	var out []*scheduler.Leader
	for _, l := range ls {
		if l.IsLeader() {
			out = append(out, l)
		}
	}
	return out
}

func killLeaderConn(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `SELECT pg_terminate_backend(l.pid) FROM pg_locks l
		WHERE l.locktype = 'advisory' AND l.classid = 0 AND l.objid = 42 AND l.granted`)
	require.NoError(t, err)
}

// TestLeaderAndCron covers leader election, failover and the cron double-fire guard.
func TestLeaderAndCron(t *testing.T) {
	pool := needDB(t)
	st := store.New(pool, testCfg)
	ctx := context.Background()
	tenant, queue := fixture(t, pool)
	c, err := st.CreateCron(ctx, store.Cron{TenantID: tenant, Name: "every", Expr: "@every 1m", Queue: queue,
		Type: "noop", Payload: json.RawMessage(`{}`), Enabled: true})
	require.NoError(t, err)
	var due time.Time
	require.NoError(t, pool.QueryRow(ctx, `UPDATE cron_schedules SET next_run_at = now() - interval '1 second'
		WHERE id = $1 RETURNING next_run_at`, c.ID).Scan(&due))
	countJobs := func() (n int) {
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE cron_id = $1`, c.ID).Scan(&n))
		return n
	}

	ls := startLeaders(t, st, 3)
	require.Eventually(t, func() bool { return len(leaders(ls)) == 1 }, 3*time.Second, 50*time.Millisecond)
	time.Sleep(3 * time.Second)
	assert.Len(t, leaders(ls), 1, "exactly one leader")
	assert.Equal(t, 1, countJobs())
	id, err := st.LeaderID(ctx)
	require.NoError(t, err)
	assert.Equal(t, id[:6], "sched-")

	// Simulate a double fire across a leader change: the schedule is due again for the same slot.
	_, err = pool.Exec(ctx, `UPDATE cron_schedules SET next_run_at = $2 WHERE id = $1`, c.ID, due)
	require.NoError(t, err)
	old := leaders(ls)[0]
	killLeaderConn(t, pool)
	require.Eventually(t, func() bool {
		cur := leaders(ls)
		return len(cur) == 1 && cur[0] != old
	}, 10*time.Second, 50*time.Millisecond, "leadership moves within 10 s")
	time.Sleep(2 * time.Second)
	assert.Equal(t, 1, countJobs())
}
