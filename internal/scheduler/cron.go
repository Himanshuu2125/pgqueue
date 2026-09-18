package scheduler

import "context"

// cron fires due schedules.
func (l *Leader) cron(ctx context.Context) {
	n, err := l.st.FireDueCron(ctx)
	if n > 0 {
		l.log.Info("cron fired", "count", n)
	}
	if err != nil && ctx.Err() == nil {
		l.log.Error("cron tick failed", "error", err)
	}
}
