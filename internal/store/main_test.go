package store_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/Himanshuu2125/pgqueue/migrations"
)

// dsn is the per-package test database; empty under -short.
var dsn string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	d, cleanup, err := startPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration tests:", err)
		os.Exit(1)
	}
	if err := migrations.Up(d); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		cleanup()
		os.Exit(1)
	}
	dsn = d
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// startPostgres starts a Postgres container for this package.
func startPostgres(ctx context.Context) (string, func(), error) {
	c, err := tcpg.Run(ctx, "postgres:16-alpine", tcpg.WithDatabase("queue"), tcpg.WithUsername("queue"),
		tcpg.WithPassword("queue"), tcpg.BasicWaitStrategies())
	if err != nil {
		return "", nil, fmt.Errorf("start postgres container: %w", err)
	}
	d, err := c.ConnectionString(ctx, "sslmode=disable")
	return d, func() { testcontainers.TerminateContainer(c) }, err //nolint:errcheck
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

func needDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() || dsn == "" {
		t.Skip("integration test: needs Postgres")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// fixture creates a unique tenant and queue for one test.
func fixture(t *testing.T, pool *pgxpool.Pool) (tenant, queue string) {
	t.Helper()
	tenant, queue = "t"+randHex(6), "q"+randHex(6)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `INSERT INTO tenants(id, api_key_hash, rate_limit_per_sec, burst) VALUES ($1, $2, 1000, 1000)`,
		tenant, []byte(randHex(16))); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO queues(name) VALUES ($1)`, queue); err != nil {
		t.Fatal(err)
	}
	return tenant, queue
}
