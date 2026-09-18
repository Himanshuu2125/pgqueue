// Command worker runs the worker pool, heartbeats and the leader-elected scheduler loops.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/handlers"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
	"github.com/Himanshuu2125/pgqueue/internal/scheduler"
	"github.com/Himanshuu2125/pgqueue/internal/store"
	"github.com/Himanshuu2125/pgqueue/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	host, _ := os.Hostname()
	b := make([]byte, 4)
	rand.Read(b) //nolint:errcheck
	workerID := host + "-" + hex.EncodeToString(b)
	observability.SetupLogging("worker", workerID, cfg.LogLevel)
	if err := run(cfg, workerID); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}
}

// connect retries the database for 30 s at startup.
func connect(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pcfg.MaxConns = int32(cfg.DBMaxConns)
	deadline := time.Now().Add(30 * time.Second)
	for {
		pool, err := pgxpool.NewWithConfig(ctx, pcfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
		slog.Warn("postgres not ready, retrying", "error", err)
		time.Sleep(time.Second)
	}
}

func run(cfg config.Config, workerID string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	shutdownTracing, err := observability.SetupTracing(ctx, "jobqueue-worker", cfg.OTLPEndpoint, cfg.SamplerRatio)
	if err != nil {
		return err
	}
	pool, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool, cfg)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) }) //nolint:errcheck
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if pool.Ping(pctx) != nil {
			http.Error(w, `{"postgres":"error"}`, http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"postgres":"ok"}`)) //nolint:errcheck
	})
	mux.Handle("/metrics", promhttp.Handler())
	hs := &http.Server{Addr: cfg.WorkerAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := hs.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("metrics server failed", "error", err)
		}
	}()

	leaderCtx, stopLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	leader := scheduler.NewLeader(cfg.DatabaseURL, workerID, st, cfg.ReaperInterval)
	go func() { defer close(leaderDone); leader.Run(leaderCtx) }()

	client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	reg := handlers.NewRegistry(pool, client, cfg.Handlers)
	p := worker.New(worker.Config{WorkerID: workerID, Queues: cfg.WorkerQueues, Concurrency: cfg.WorkerConcurrency,
		Poll: cfg.PollInterval, Lease: cfg.Lease, Heartbeat: cfg.Heartbeat, ShutdownGrace: cfg.ShutdownGrace,
		BackoffBase: cfg.BackoffBase, BackoffMax: cfg.BackoffMax}, st, reg.Run)
	slog.Info("worker started", "queues", cfg.WorkerQueues, "concurrency", cfg.WorkerConcurrency)
	runErr := p.Run(ctx)
	stopLeader()
	<-leaderDone
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	hs.Shutdown(sctx) //nolint:errcheck
	if err := shutdownTracing(sctx); err != nil {
		slog.Warn("tracer flush failed", "error", err)
	}
	return runErr
}
