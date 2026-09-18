package scheduler

import (
	"context"

	"github.com/Himanshuu2125/pgqueue/internal/observability"
)

// reap reclaims expired leases.
func (l *Leader) reap(ctx context.Context) {
	n, err := l.st.ReapExpired(ctx)
	if n > 0 {
		observability.Reclaimed.Add(float64(n))
		l.log.Info("reaper reclaimed expired leases", "count", n)
	}
	if err != nil && ctx.Err() == nil {
		l.log.Error("reaper failed", "error", err)
	}
}
