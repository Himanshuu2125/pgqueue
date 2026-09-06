// Package config loads and validates all configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Tenant is one SEED_TENANTS entry.
type Tenant struct {
	ID, Key     string
	Rate, Burst int
}

// Handlers is the configuration the job handlers need.
type Handlers struct {
	AllowedHosts []string
	QueueToken   string
}

// Config holds every env var from .env.example.
type Config struct {
	DatabaseURL, RedisURL, APIAddr, WorkerAddr string
	DBMaxConns                                 int
	MigrateOnStart                             bool
	SeedTenants                                []Tenant
	WorkerConcurrency                          int
	WorkerQueues                               []string
	PollInterval, Lease, Heartbeat             time.Duration
	ReaperInterval, ShutdownGrace              time.Duration
	BackoffBase, BackoffMax                    time.Duration
	DefaultMaxAttempts, DefaultTimeoutSeconds  int
	Handlers                                   Handlers
	AdminUser, AdminPassword, LogLevel         string
	OTLPEndpoint                               string
	SamplerRatio                               float64
}

var tenantIDRe = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)

type loader struct{ errs []error }

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

func (l *loader) int(key string, def int) int {
	v := l.str(key, strconv.Itoa(def))
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: not an integer: %q", key, v))
	}
	return n
}

func (l *loader) check(ok bool, msg string) {
	if !ok {
		l.errs = append(l.errs, errors.New(msg))
	}
}

func list(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func ms(n int) time.Duration  { return time.Duration(n) * time.Millisecond }
func sec(n int) time.Duration { return time.Duration(n) * time.Second }

// Load reads and validates the environment.
func Load() (Config, error) {
	l := &loader{}
	c := Config{
		DatabaseURL:           l.str("DATABASE_URL", "postgres://queue:queue@postgres:5432/queue?sslmode=disable"),
		RedisURL:              l.str("REDIS_URL", "redis://redis:6379/0"),
		APIAddr:               l.str("API_ADDR", ":8080"),
		WorkerAddr:            l.str("WORKER_ADDR", ":9091"),
		DBMaxConns:            l.int("DB_MAX_CONNS", 20),
		MigrateOnStart:        l.str("MIGRATE_ON_START", "true") == "true",
		WorkerConcurrency:     l.int("WORKER_CONCURRENCY", 16),
		WorkerQueues:          list(l.str("WORKER_QUEUES", "default")),
		PollInterval:          ms(l.int("POLL_INTERVAL_MS", 200)),
		Lease:                 sec(l.int("LEASE_SECONDS", 30)),
		Heartbeat:             sec(l.int("HEARTBEAT_SECONDS", 10)),
		ReaperInterval:        sec(l.int("REAPER_INTERVAL_SECONDS", 5)),
		ShutdownGrace:         sec(l.int("SHUTDOWN_GRACE_SECONDS", 25)),
		BackoffBase:           ms(l.int("BACKOFF_BASE_MS", 1000)),
		BackoffMax:            ms(l.int("BACKOFF_MAX_MS", 300000)),
		DefaultMaxAttempts:    l.int("DEFAULT_MAX_ATTEMPTS", 5),
		DefaultTimeoutSeconds: l.int("DEFAULT_TIMEOUT_SECONDS", 60),
		Handlers: Handlers{
			AllowedHosts: list(strings.ToLower(l.str("HTTP_HANDLER_ALLOWED_HOSTS", ""))),
			QueueToken:   l.str("HTTP_HANDLER_QUEUE_TOKEN", ""),
		},
		AdminUser:     l.str("ADMIN_USER", "admin"),
		AdminPassword: l.str("ADMIN_PASSWORD", ""),
		LogLevel:      l.str("LOG_LEVEL", "info"),
		OTLPEndpoint:  l.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	}
	ratio, err := strconv.ParseFloat(l.str("OTEL_TRACES_SAMPLER_RATIO", "1.0"), 64)
	l.check(err == nil && ratio >= 0 && ratio <= 1, "OTEL_TRACES_SAMPLER_RATIO must be in [0,1]")
	c.SamplerRatio = ratio

	for _, e := range list(l.str("SEED_TENANTS", "")) {
		t, err := parseTenant(e)
		if err != nil {
			l.errs = append(l.errs, err)
			continue
		}
		c.SeedTenants = append(c.SeedTenants, t)
	}
	l.check(c.AdminPassword != "", "ADMIN_PASSWORD must be non-empty")
	l.check(c.Heartbeat*3 <= c.Lease, "HEARTBEAT_SECONDS * 3 must be <= LEASE_SECONDS")
	l.check(c.ShutdownGrace+3*time.Second < 30*time.Second, "SHUTDOWN_GRACE_SECONDS + 3 must be < 30")
	l.check(c.WorkerConcurrency >= 1 && c.WorkerConcurrency <= 256, "WORKER_CONCURRENCY must be in 1..256")
	l.check(c.BackoffBase <= c.BackoffMax, "BACKOFF_BASE_MS must be <= BACKOFF_MAX_MS")
	l.check(c.DefaultMaxAttempts >= 1 && c.DefaultMaxAttempts <= 25, "DEFAULT_MAX_ATTEMPTS must be in 1..25")
	l.check(c.DefaultTimeoutSeconds >= 1 && c.DefaultTimeoutSeconds <= 3600, "DEFAULT_TIMEOUT_SECONDS must be in 1..3600")
	l.check(len(c.WorkerQueues) > 0, "WORKER_QUEUES must be non-empty")
	return c, errors.Join(l.errs...)
}

func parseTenant(e string) (Tenant, error) {
	p := strings.Split(e, ":")
	if len(p) != 4 {
		return Tenant{}, fmt.Errorf("SEED_TENANTS entry %d fields, want id:key:rate:burst", len(p))
	}
	rate, err1 := strconv.Atoi(p[2])
	burst, err2 := strconv.Atoi(p[3])
	switch {
	case !tenantIDRe.MatchString(p[0]):
		return Tenant{}, fmt.Errorf("SEED_TENANTS: invalid tenant id %q", p[0])
	case len(p[1]) < 16:
		return Tenant{}, fmt.Errorf("SEED_TENANTS: key for %q shorter than 16 chars", p[0])
	case err1 != nil || err2 != nil || rate < 1 || burst < 1:
		return Tenant{}, fmt.Errorf("SEED_TENANTS: rate and burst for %q must be integers >= 1", p[0])
	}
	return Tenant{ID: p[0], Key: p[1], Rate: rate, Burst: burst}, nil
}
