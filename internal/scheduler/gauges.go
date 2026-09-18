package scheduler

import (
	"context"

	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
)

// gauges refreshes queue_depth and queue_oldest_pending_age_seconds from one GROUP BY query.
func (l *Leader) gauges(ctx context.Context) {
	rows, err := l.st.Depth(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.log.Error("depth query failed", "error", err)
		}
		return
	}
	queues, err := l.st.ListQueues(ctx)
	if err != nil {
		if ctx.Err() == nil {
			l.log.Error("list queues failed", "error", err)
		}
		return
	}
	observability.Depth.Reset()
	observability.OldestPending.Reset()
	for _, q := range queues { // zero-fill so every queue/state has a series
		for _, st := range []string{model.StateScheduled, model.StatePending, model.StateRunning, model.StateDead} {
			observability.Depth.WithLabelValues(q.Name, st).Set(0)
		}
		observability.OldestPending.WithLabelValues(q.Name).Set(0)
	}
	for _, r := range rows {
		observability.Depth.WithLabelValues(r.Queue, r.State).Set(float64(r.Count))
		if r.State == model.StatePending {
			observability.OldestPending.WithLabelValues(r.Queue).Set(r.OldestAge)
		}
	}
}
