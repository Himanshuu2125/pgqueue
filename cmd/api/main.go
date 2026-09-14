// Command api serves the HTTP API and admin UI, runs migrations and seeds tenants.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Himanshuu2125/pgqueue/internal/api"
	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/handlers"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
	"github.com/Himanshuu2125/pgqueue/internal/store"
	"github.com/Himanshuu2125/pgqueue/migrations"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	observability.SetupLogging("api", "", cfg.LogLevel)
	if err := run(cfg); err != nil {
		slog.Error("api exited", "error", err)
		os.Exit(1)
	}
}

// connect migrates (if enabled) and opens the pool, retrying for 60 s so the api
// can start alongside Postgres.
func connect(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pcfg.MaxConns = int32(cfg.DBMaxConns)
	try := func() (*pgxpool.Pool, error) {
		if cfg.MigrateOnStart {
			if err := migrations.Up(cfg.DatabaseURL); err != nil {
				return nil, err
			}
		}
		pool, err := pgxpool.NewWithConfig(ctx, pcfg)
		if err == nil {
			if err = pool.Ping(ctx); err != nil {
				pool.Close()
			}
		}
		if err != nil {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
		return pool, nil
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		pool, err := try()
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return pool, err
		}
		slog.Warn("postgres not ready, retrying", "error", err)
		time.Sleep(time.Second)
	}
}

func run(cfg config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	shutdownTracing, err := observability.SetupTracing(ctx, "jobqueue-api", cfg.OTLPEndpoint, cfg.SamplerRatio)
	if err != nil {
		return err
	}
	pool, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool, cfg)
	if err := st.SeedTenants(ctx, cfg.SeedTenants); err != nil {
		return err
	}
	ropt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(ropt)
	defer rdb.Close()
	srv, err := api.New(cfg, st, handlers.NewRegistry(nil, nil, cfg.Handlers), rdb)
	if err != nil {
		return err
	}
	hs := &http.Server{Addr: cfg.APIAddr, Handler: srv.Router(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		hs.Shutdown(sctx) //nolint:errcheck
	}()
	slog.Info("api listening", "addr", cfg.APIAddr)
	if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return shutdownTracing(context.Background())
}
