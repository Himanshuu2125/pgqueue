package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/Himanshuu2125/pgqueue/internal/observability"
)

func TestTokenBucketMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	l := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	ctx := context.Background()

	ok, _ := l.Allow(ctx, "a", 1, 3, 3)
	assert.True(t, ok, "full bucket allows burst")
	before := testutil.ToFloat64(observability.RateLimited.WithLabelValues("a"))
	ok, retry := l.Allow(ctx, "a", 1, 3, 2)
	assert.False(t, ok, "empty bucket denies")
	assert.GreaterOrEqual(t, retry, 1)
	assert.LessOrEqual(t, retry, 2)
	assert.Equal(t, before+1, testutil.ToFloat64(observability.RateLimited.WithLabelValues("a")))

	ok, retry = l.Allow(ctx, "b", 10, 5, 6)
	assert.False(t, ok, "cost > burst is denied")
	assert.Equal(t, 0, retry, "no Retry-After when cost > burst")

	assert.Greater(t, mr.TTL("rl:a"), time.Duration(0), "bucket key expires")
}

func TestTokenBucketFailOpen(t *testing.T) {
	mr := miniredis.RunT(t)
	l := New(redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1}))
	mr.Close()
	before := testutil.ToFloat64(observability.RateLimitErrors)
	ok, _ := l.Allow(context.Background(), "a", 1, 1, 1)
	assert.True(t, ok)
	assert.Equal(t, before+1, testutil.ToFloat64(observability.RateLimitErrors))
}

func TestTokenBucketRealRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	t.Cleanup(func() { c.Terminate(context.Background()) }) //nolint:errcheck
	url, err := c.ConnectionString(ctx)
	require.NoError(t, err)
	opt, err := redis.ParseURL(url)
	require.NoError(t, err)
	rdb := redis.NewClient(opt)
	tenant := fmt.Sprintf("real-%d", time.Now().UnixNano())
	l := New(rdb)
	ok, _ := l.Allow(ctx, tenant, 1, 2, 2)
	require.True(t, ok)
	ok, retry := l.Allow(ctx, tenant, 1, 2, 1)
	assert.False(t, ok, "429")
	assert.Equal(t, 1, retry)
}
