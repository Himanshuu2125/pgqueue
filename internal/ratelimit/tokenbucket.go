// Package ratelimit is a Redis token bucket per tenant.
package ratelimit

import (
	"context"
	_ "embed"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
)

//go:embed tokenbucket.lua
var script string

// Limiter charges submits against a tenant's bucket and fails open on Redis errors.
type Limiter struct {
	rdb    redis.Scripter
	script *redis.Script
}

// New builds a Limiter on rdb.
func New(rdb redis.Scripter) *Limiter { return &Limiter{rdb: rdb, script: redis.NewScript(script)} }

// Allow charges cost tokens. retryAfterSec is 0 when no Retry-After header should be sent.
func (l *Limiter) Allow(ctx context.Context, tenant string, rate, burst, cost int) (ok bool, retryAfterSec int) {
	if cost > burst {
		return false, 0
	}
	ctx, cancel := context.WithTimeout(ctx, config.RateLimitRedisTimeout)
	defer cancel()
	res, err := l.script.Run(ctx, l.rdb, []string{"rl:" + tenant}, rate, burst, cost).Int64Slice()
	if err != nil || len(res) != 2 {
		slog.WarnContext(ctx, "rate limiter unavailable, failing open", "tenant_id", tenant, "error", err)
		observability.RateLimitErrors.Inc()
		return true, 0
	}
	if res[0] == 1 {
		return true, 0
	}
	observability.RateLimited.WithLabelValues(tenant).Inc()
	return false, int((res[1] + 999) / 1000)
}
