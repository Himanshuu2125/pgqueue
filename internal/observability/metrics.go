// Package observability holds Prometheus metrics, tracing and logging setup.
package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	durBuckets  = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}
	waitBuckets = []float64{.001, .005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
)

func counter(name, help string, labels ...string) *prometheus.CounterVec {
	return promauto.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
}

func histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	return promauto.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
}

// API metrics.
var (
	JobsSubmitted   = counter("queue_jobs_submitted_total", "Jobs created by submit.", "queue", "type")
	RateLimited     = counter("queue_ratelimited_total", "Submits rejected by the rate limiter.", "tenant")
	RateLimitErrors = promauto.NewCounter(prometheus.CounterOpts{Name: "queue_ratelimit_errors_total", Help: "Rate limiter Redis errors (failed open)."})
	HTTPRequests    = counter("queue_http_requests_total", "HTTP requests.", "route", "code")
	HTTPDuration    = histogram("queue_http_request_duration_seconds", "HTTP request duration.", prometheus.DefBuckets, "route")
)

// Worker metrics.
var (
	JobsCompleted = counter("queue_jobs_completed_total", "Job executions by outcome.", "queue", "type", "outcome")
	JobDuration   = histogram("queue_job_duration_seconds", "Handler duration.", durBuckets, "queue", "type")
	JobWait       = histogram("queue_job_wait_seconds", "started_at - run_at, observed at claim.", waitBuckets, "queue")
	Retries       = counter("queue_retries_total", "Retries scheduled.", "queue")
	StaleAcks     = promauto.NewCounter(prometheus.CounterOpts{Name: "queue_stale_acks_total", Help: "Acks rejected by fencing."})
	LeasesLost    = promauto.NewCounter(prometheus.CounterOpts{Name: "queue_leases_lost_total", Help: "Leases lost at heartbeat."})
	BusySlots     = promauto.NewGauge(prometheus.GaugeOpts{Name: "queue_worker_busy_slots", Help: "Handlers currently running."})
	ClaimLatency  = promauto.NewHistogram(prometheus.HistogramOpts{Name: "queue_claim_latency_seconds", Help: "Claim query latency.", Buckets: prometheus.DefBuckets})
)

// Leader metrics.
var (
	Depth           = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "queue_depth", Help: "Jobs by queue and state."}, []string{"queue", "state"})
	OldestPending   = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "queue_oldest_pending_age_seconds", Help: "Age of the oldest runnable pending job."}, []string{"queue"})
	Reclaimed       = promauto.NewCounter(prometheus.CounterOpts{Name: "queue_reclaimed_total", Help: "Expired leases reclaimed by the reaper."})
	SchedulerLeader = promauto.NewGauge(prometheus.GaugeOpts{Name: "queue_scheduler_leader", Help: "1 if this worker is the scheduler leader."})
)
