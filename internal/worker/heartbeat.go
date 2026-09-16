package worker

import (
	"context"
	"time"

	"github.com/Himanshuu2125/pgqueue/internal/observability"
)

func (p *Pool) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(p.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.beat(ctx)
		}
	}
}

// beat extends every held lease; lost leases and cancel requests cancel their handlers.
func (p *Pool) beat(ctx context.Context) {
	leases := p.leases()
	if len(leases) == 0 {
		return
	}
	lost, cancel, err := p.st.Heartbeat(ctx, leases, p.cfg.Lease)
	if err != nil {
		p.log.ErrorContext(ctx, "heartbeat failed", "error", err)
		return
	}
	for _, id := range lost {
		if a := p.get(id); a != nil {
			a.lost.Store(true)
			a.cancel()
			p.log.WarnContext(ctx, "lease_lost", "job_id", id, "lease_token", a.lease.Token)
			observability.LeasesLost.Inc()
		}
	}
	for _, id := range cancel {
		if a := p.get(id); a != nil {
			a.cancelReq.Store(true)
			a.cancel()
		}
	}
}
