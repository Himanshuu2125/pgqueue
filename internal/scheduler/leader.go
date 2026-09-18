// Package scheduler runs the leader-only loops: reaper, cron and depth gauges.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
	"github.com/Himanshuu2125/pgqueue/internal/store"
)

// Leader holds pg_advisory_lock(42) on a dedicated connection and runs the scheduler loops while it does.
type Leader struct {
	dsn, workerID  string
	st             *store.Postgres
	reaperInterval time.Duration
	leader         atomic.Bool
	log            *slog.Logger
}

// NewLeader builds a Leader.
func NewLeader(dsn, workerID string, st *store.Postgres, reaperInterval time.Duration) *Leader {
	return &Leader{dsn: dsn, workerID: workerID, st: st, reaperInterval: reaperInterval, log: slog.Default()}
}

// IsLeader reports whether this instance currently holds the lock.
func (l *Leader) IsLeader() bool { return l.leader.Load() }

// Run campaigns every LeaderRetryInterval until ctx is cancelled, then releases leadership.
func (l *Leader) Run(ctx context.Context) {
	observability.SchedulerLeader.Set(0)
	var conn *pgx.Conn
	var stop func()
	lose := func(err error) {
		if stop != nil {
			stop()
			stop = nil
		}
		if l.leader.Swap(false) && err != nil {
			l.log.Warn("scheduler leadership lost", "error", err)
		}
		observability.Depth.Reset()
		observability.OldestPending.Reset()
		observability.SchedulerLeader.Set(0)
		if conn != nil {
			conn.Close(context.Background()) //nolint:errcheck
			conn = nil
		}
	}
	defer lose(nil)
	t := time.NewTicker(config.LeaderRetryInterval)
	defer t.Stop()
	for {
		if err := l.tick(ctx, &conn, &stop); err != nil && ctx.Err() == nil {
			lose(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (l *Leader) tick(ctx context.Context, conn **pgx.Conn, stop *func()) error {
	if *conn == nil {
		cfg, err := pgx.ParseConfig(l.dsn)
		if err != nil {
			return fmt.Errorf("leader dsn: %w", err)
		}
		cfg.RuntimeParams["application_name"] = l.workerID
		if *conn, err = pgx.ConnectConfig(ctx, cfg); err != nil {
			return fmt.Errorf("leader connect: %w", err)
		}
	}
	qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if l.leader.Load() {
		if _, err := (*conn).Exec(qctx, "SELECT 1"); err != nil {
			return fmt.Errorf("leader ping: %w", err)
		}
		return nil
	}
	var got bool
	if err := (*conn).QueryRow(qctx, "SELECT pg_try_advisory_lock($1)", config.LeaderLockID).Scan(&got); err != nil {
		return fmt.Errorf("leader lock: %w", err)
	}
	if got {
		l.leader.Store(true)
		observability.SchedulerLeader.Set(1)
		l.log.Info("scheduler leadership acquired")
		*stop = l.startLoops(ctx)
	}
	return nil
}

func (l *Leader) startLoops(ctx context.Context) func() {
	sctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, loop := range []struct {
		every time.Duration
		fn    func(context.Context)
	}{{l.reaperInterval, l.reap}, {config.CronTick, l.cron}, {config.GaugeInterval, l.gauges}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(loop.every)
			defer t.Stop()
			for {
				loop.fn(sctx)
				select {
				case <-sctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	return func() { cancel(); wg.Wait() }
}
