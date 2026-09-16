// Package worker runs the claim loop, handler goroutines, heartbeats and graceful shutdown.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Himanshuu2125/pgqueue/internal/config"
	"github.com/Himanshuu2125/pgqueue/internal/handlers"
	"github.com/Himanshuu2125/pgqueue/internal/model"
	"github.com/Himanshuu2125/pgqueue/internal/observability"
	"github.com/Himanshuu2125/pgqueue/internal/store"
)

// Config is the worker pool configuration.
type Config struct {
	WorkerID                string
	Queues                  []string
	Concurrency             int
	Poll, Lease, Heartbeat  time.Duration
	ShutdownGrace           time.Duration
	BackoffBase, BackoffMax time.Duration
}

type active struct {
	lease           store.Lease
	cancel          context.CancelFunc
	lost, cancelReq atomic.Bool
}

// Pool claims and executes jobs.
type Pool struct {
	cfg      Config
	st       store.WorkerStore
	run      handlers.Handler
	log      *slog.Logger
	mu       sync.Mutex
	active   map[uuid.UUID]*active
	rngMu    sync.Mutex
	rng      *rand.Rand
	slots    chan struct{}
	freed    chan struct{}
	wg       sync.WaitGroup
	shutdown atomic.Bool
}

// New builds a Pool.
func New(cfg Config, st store.WorkerStore, run handlers.Handler) *Pool {
	return &Pool{cfg: cfg, st: st, run: run, log: slog.Default(), active: map[uuid.UUID]*active{},
		rng:   rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		slots: make(chan struct{}, cfg.Concurrency), freed: make(chan struct{}, 1)}
}

// Run claims jobs until ctx is cancelled, then shuts down gracefully.
func (p *Pool) Run(ctx context.Context) error {
	base := context.WithoutCancel(ctx)
	jobsCtx, cancelJobs := context.WithCancel(base)
	defer cancelJobs()
	hbCtx, stopHB := context.WithCancel(base)
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); p.heartbeatLoop(hbCtx) }()

	p.claimLoop(ctx, jobsCtx)
	p.shutdown.Store(true)
	p.log.Info("shutdown: waiting for handlers", "in_flight", len(p.slots))
	if !waitTimeout(&p.wg, p.cfg.ShutdownGrace) {
		cancelJobs()
		waitTimeout(&p.wg, config.ShutdownExtraWait)
	}
	stopHB()
	<-hbDone
	leases := p.leases()
	rctx, cancel := context.WithTimeout(base, 5*time.Second)
	defer cancel()
	if err := p.st.Release(rctx, leases); err != nil {
		return err
	}
	p.log.Info("shutdown: released leases", "count", len(leases))
	return nil
}

func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func (p *Pool) sleepJitter(ctx context.Context) {
	p.rngMu.Lock()
	d := time.Duration(float64(p.cfg.Poll) * (0.8 + 0.4*p.rng.Float64()))
	p.rngMu.Unlock()
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func (p *Pool) claimLoop(ctx, jobsCtx context.Context) {
	for ctx.Err() == nil {
		n := min(cap(p.slots)-len(p.slots), config.ClaimBatchMax)
		if n == 0 {
			select {
			case <-p.freed:
			case <-ctx.Done():
			}
			continue
		}
		start := time.Now()
		// A claim that reached the DB must be returned to us, so it is not cancelled by shutdown.
		jobs, err := p.st.Claim(context.WithoutCancel(ctx), p.cfg.WorkerID, p.cfg.Queues, n, p.cfg.Lease)
		observability.ClaimLatency.Observe(time.Since(start).Seconds())
		if err != nil {
			p.log.ErrorContext(ctx, "claim failed", "error", err)
		}
		for _, j := range jobs {
			p.slots <- struct{}{}
			p.wg.Add(1)
			jctx, cancel := context.WithCancel(jobsCtx)
			a := &active{lease: store.Lease{JobID: j.ID, Token: *j.LeaseToken}, cancel: cancel}
			p.mu.Lock()
			p.active[j.ID] = a
			p.mu.Unlock()
			go p.execute(jctx, j, a)
		}
		if len(jobs) == 0 {
			p.sleepJitter(ctx)
		}
	}
}

func (p *Pool) get(id uuid.UUID) *active {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active[id]
}

func (p *Pool) forget(id uuid.UUID) {
	p.mu.Lock()
	delete(p.active, id)
	p.mu.Unlock()
}

func (p *Pool) leases() []store.Lease {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]store.Lease, 0, len(p.active))
	for _, a := range p.active {
		out = append(out, a.lease)
	}
	return out
}

func (p *Pool) execute(jctx context.Context, job model.Job, a *active) {
	defer func() {
		a.cancel()
		observability.BusySlots.Dec()
		<-p.slots
		select {
		case p.freed <- struct{}{}:
		default:
		}
		p.wg.Done()
	}()
	observability.BusySlots.Inc()
	if job.StartedAt != nil {
		observability.JobWait.WithLabelValues(job.Queue).Observe(job.StartedAt.Sub(job.RunAt).Seconds())
	}
	tp := ""
	if job.TraceParent != nil {
		tp = *job.TraceParent
	}
	pctx := otel.GetTextMapPropagator().Extract(jctx, propagation.MapCarrier{"traceparent": tp})
	sctx, span := otel.Tracer("jobqueue-worker").Start(pctx, "job.execute", trace.WithAttributes(
		attribute.String("job.id", job.ID.String()), attribute.String("job.type", job.Type),
		attribute.String("job.queue", job.Queue), attribute.Int("job.attempt", job.Attempts)))
	defer span.End()
	log := p.log.With("job_id", job.ID, "tenant_id", job.TenantID, "queue", job.Queue, "type", job.Type,
		"attempt", job.Attempts, "lease_token", a.lease.Token, "trace_id", observability.TraceID(sctx))

	start := time.Now()
	hctx, cancel := context.WithTimeout(sctx, time.Duration(job.TimeoutSeconds)*time.Second)
	res, err := p.run(hctx, job)
	timedOut := errors.Is(hctx.Err(), context.DeadlineExceeded) && jctx.Err() == nil
	cancel()
	dur := time.Since(start)
	observability.JobDuration.WithLabelValues(job.Queue, job.Type).Observe(dur.Seconds())
	outcome := p.finish(context.WithoutCancel(jctx), job, a, res, err, timedOut)
	log.InfoContext(sctx, "job finished", "outcome", outcome, "duration_ms", dur.Milliseconds())
}

// finish classifies a handler return and acks accordingly. Order matters: a lost lease
// is never acked, success wins over a pending cancel, and shutdown leaves the job for Release.
func (p *Pool) finish(ctx context.Context, job model.Job, a *active, res json.RawMessage, err error, timedOut bool) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var outcome string
	var ackErr error
	switch {
	case a.lost.Load():
		p.forget(job.ID)
		return "lease_lost"
	case err == nil:
		p.forget(job.ID)
		outcome, ackErr = model.StateSucceeded, p.st.Complete(ctx, a.lease, res)
	case a.cancelReq.Load():
		p.forget(job.ID)
		outcome, ackErr = model.StateCancelled, p.st.Fail(ctx, a.lease, err.Error(), nil)
	case p.shutdown.Load():
		return "released"
	case !timedOut && handlers.IsPermanent(err):
		p.forget(job.ID)
		outcome, ackErr = model.StateDead, p.st.Fail(ctx, a.lease, err.Error(), nil)
	default:
		p.forget(job.ID)
		msg := err.Error()
		if timedOut {
			msg = "timeout"
		}
		if job.Attempts < job.MaxAttempts {
			p.rngMu.Lock()
			d := Backoff(p.rng, job.Attempts, p.cfg.BackoffBase, p.cfg.BackoffMax)
			p.rngMu.Unlock()
			outcome, ackErr = "failed", p.st.Fail(ctx, a.lease, msg, &d)
			if ackErr == nil {
				observability.Retries.WithLabelValues(job.Queue).Inc()
			}
		} else {
			outcome, ackErr = model.StateDead, p.st.Fail(ctx, a.lease, msg, nil)
		}
	}
	if errors.Is(ackErr, model.ErrLeaseLost) {
		return "stale_ack"
	} else if ackErr != nil {
		p.log.ErrorContext(ctx, "ack failed", "job_id", job.ID, "error", ackErr)
		return "ack_error"
	}
	observability.JobsCompleted.WithLabelValues(job.Queue, job.Type, outcome).Inc()
	return outcome
}
