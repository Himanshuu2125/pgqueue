# Architecture

```
 clients ──HTTP──► api (cmd/api) ─────────────► PostgreSQL (jobs, events, cron, tenants, queues)
  (Bearer key)      │  /v1, /admin (htmx)              ▲          ▲
                    │  rate limit ──► Redis            │ claim /  │ reaper, cron,
                    │  /metrics                        │ heartbeat│ depth gauges
                    ▼                                  │ / ack    │
            worker × N (cmd/worker) ───────────────────┘          │
             ├─ claim loop → handler goroutines (http, noop, sleep, fail, record)
             ├─ heartbeat loop
             └─ scheduler loops ── only in the leader (pg advisory lock 42) ──┘
 Prometheus ◄─ api + workers     OTel traces ─► Jaeger     Grafana ◄─ Prometheus
```

Three binaries share one Postgres database:

- **`cmd/api`** serves the REST API and the admin UI, applies migrations at startup and seeds tenants.
- **`cmd/worker`** claims and executes jobs. One worker at a time is elected leader and also runs the reaper, the cron scheduler and the queue-depth gauges.
- **`cmd/loadgen`** is the load generator (`run`) and the chaos-test verifier (`verify`).

Postgres is the only source of truth. Redis holds nothing but rate-limiter buckets.

## Job lifecycle

```
submit ─────────────────────────────► pending   (reported as "scheduled" while run_at > now())
pending ──claim──────────────────────► running   (attempts+1, new lease_token)
running ──handler ok─────────────────► succeeded
running ──retryable error, attempts < max──► pending (run_at = now()+backoff)
running ──permanent error, or attempts = max──► dead
running ──lease expired (reaper)─────► pending (backoff) | dead (attempts = max)
running ──shutdown release───────────► pending (attempts−1, run_at = now())
pending ──cancel─────────────────────► cancelled
running ──cancel─────────────────────► cancel_requested → cancelled at the next ack/reap/release
                                       (a handler that still returns success wins)
dead ──replay────────────────────────► pending (attempts = 0)
```

Every transition writes a row to `job_events` in the same transaction, so a job's history can never disagree with its state. `scheduled` is never stored. It is derived in the `SELECT` list with the database clock, so the API and the claim query can never disagree about whether a job is runnable.

## Claiming: `FOR UPDATE SKIP LOCKED`

```sql
WITH picked AS (SELECT id FROM jobs WHERE state='pending' AND run_at <= now() AND queue = ANY($1)
                  AND queue NOT IN (SELECT name FROM queues WHERE paused)
                ORDER BY priority DESC, run_at FOR UPDATE SKIP LOCKED LIMIT $2),
     upd AS (UPDATE jobs ... state='running', attempts+1, lease_expires_at = now()+lease,
             lease_token = nextval('lease_seq') ... RETURNING j.*),
     ev  AS (INSERT INTO job_events ... 'claimed')
SELECT ... FROM upd;
```

- `FOR UPDATE` makes it impossible for two workers to lease the same row. `SKIP LOCKED` means they also don't *wait* for each other: a worker skips rows another transaction is claiming and takes the next ones. Without it, every worker would queue up behind the same head-of-queue rows. With it, claim throughput grows with the number of workers.
- Picking, updating and writing the event happen in one statement, so a claim is never visible without its event.
- The partial index `jobs_claim (priority DESC, run_at) WHERE state='pending'` serves the ordering. It stays small because finished jobs drop out of it.
- Each worker process has a single claim loop. It asks for `min(free_slots, 10)` jobs, so it never holds more leases than it can run. When every slot is busy it blocks on a slot-freed channel. When the queue is empty it sleeps `POLL_INTERVAL_MS ± 20%`, and the jitter keeps workers from polling in lockstep.
- The claim query runs on a context that shutdown does not cancel. Rows that a claim has already updated must come back to the worker; otherwise they would sit leased by nobody until the lease expired.
- Ordering is global (`priority DESC, run_at`) across all of a worker's queues, so a busy high-priority queue can starve a low-priority one. That is a deliberate simplification: there are no per-queue concurrency limits or fairness rules.

## Leases, heartbeats and fencing

All lease and schedule timestamps come from Postgres `now()`. No Go code computes a lease expiry or compares wall-clock time against one, so clock skew between machines doesn't matter.

```
t=0     claim: lease_expires_at = now()+30s, lease_token = 17 (lease_seq, globally increasing)
t=10    heartbeat: UPDATE ... SET lease_expires_at = now()+30s WHERE id=… AND lease_token=17 AND state='running'
t=20    heartbeat (same)
t=25    handler returns → Complete ... WHERE id=… AND lease_token=17 AND state='running'  → 1 row: acked
```

If a worker stalls (GC pause, network partition, SIGKILL) past its lease:

```
t=0     worker A claims, token 17
t=35    lease expired; reaper → state=pending, reclaimed event
t=37    worker B claims → token 18
t=40    A wakes up and acks with token 17 → 0 rows → ErrLeaseLost, queue_stale_acks_total++
```

The lease token is a **fencing token**. Every write that ends an attempt (Complete, Fail, Release, heartbeat) is conditioned on `lease_token = <mine> AND state = 'running'`, so a stale worker can never overwrite a newer attempt.

One heartbeat statement per worker extends all of that worker's leases at once. An id that is missing from the heartbeat's result means the lease was lost: that handler's context is cancelled and the job is never acked. A heartbeat *error*, by contrast, marks nothing lost, because a transient DB blip should not abort healthy work. The startup check `HEARTBEAT_SECONDS * 3 <= LEASE_SECONDS` means a worker can miss two heartbeats in a row without losing its leases.

## Handler outcomes and retries

After a handler returns, the worker classifies the result in this order:

1. The lease was lost → no ack.
2. The handler returned nil → Complete. Success wins even if a cancel was requested.
3. A cancel was requested → the job becomes `cancelled`.
4. Shutdown is in progress → no ack; the job is released (see below).
5. The per-job timeout fired → retryable, with `last_error = "timeout"`.
6. The error is marked permanent → `dead`.
7. Anything else → retryable.

A retryable failure is retried while `attempts < max_attempts`, after a full-jitter backoff of `uniform[0, min(BACKOFF_MAX_MS, BACKOFF_BASE_MS · 2^(attempts−1))]`. The shift is capped at 30, so the calculation cannot overflow.

The `http` handler only calls hosts on an exact-match allowlist. Redirects are disabled, and the response body read is capped at 64 KiB. A 4xx other than 408/409/429 is permanent. A 408, 409, 429, any 5xx and network errors are retried.

## Reaper vs. ack race

The reaper runs on the leader every `REAPER_INTERVAL_SECONDS`. It selects expired leases `FOR UPDATE SKIP LOCKED` in batches of 500.

- If an ack is committing on a row at that moment, it holds the row lock. The reaper skips that row, and the ack wins.
- If the reaper wins, the late ack's fenced `WHERE` matches nothing, so the ack becomes a counted stale ack and the job is retried.

Neither ordering loses the job or leaves it half-updated. The worst case is one extra execution. Reclaimed jobs get the same jittered backoff as normal retries, and go to `dead` once `attempts >= max_attempts`.

## Leader election and split brain

- Each worker opens a dedicated connection (outside the pool) with `application_name = worker_id`. Every 5 s it tries `pg_try_advisory_lock(42)`. Whoever holds the lock runs the reaper, cron and gauge loops. Session-level advisory locks are released when the session ends, so a crashed leader loses the lock as soon as Postgres notices the dead connection.
- The leader pings its connection every 5 s (`SELECT 1`, 2 s timeout). On any error it stops its loops, resets its depth gauges, sets `queue_scheduler_leader` to 0 and reconnects.
- **Split brain is tolerated rather than prevented.** For a moment two workers may both think they are leader, and that is harmless. The reaper uses `SKIP LOCKED` and re-checks expiry inside its update. Cron enqueues with the idempotency key `cron:{schedule_id}:{next_run_at unix}`, so two leaders firing the same slot still create one job.
- The admin dashboard finds the current leader by joining `pg_locks` with `pg_stat_activity`.

## Cron

Schedules are standard 5-field expressions or descriptors (`@hourly`, `@every 5m`), always evaluated in UTC from the database's `now()`. Time-zone prefixes and `@every` intervals under one minute are rejected. Each due schedule is fired in its own transaction: enqueue the job, then advance `next_run_at`. Missed runs fire once; they are not backfilled.

## Idempotency

- `INSERT … ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING …`. If no row comes back, the existing row is read in the same READ COMMITTED transaction and returned with HTTP 200 instead of 201. The body of a duplicate is ignored, and it writes no event.
- Concurrent identical submits are safe. The unique index makes the second insert wait for the first to commit and then do nothing. Its follow-up `SELECT` runs as a new statement, so it sees the committed row.
- NULL keys never conflict. The `cron:` prefix is reserved for the scheduler.
- Batch submits run in one transaction. The first invalid item rejects the whole batch and is reported by `index`.

## Rate limiting

There is one token bucket per tenant, implemented as a Lua script over a Redis hash. The script uses Redis `TIME`, so refill and deduction are atomic and don't depend on API host clocks. A batch costs one token per job, and idempotent duplicates are still charged, so client retry loops can't get around the limit.

The limiter **fails open**. On any Redis error, or after a 100 ms timeout, the request is allowed and `queue_ratelimit_errors_total` is incremented. Losing Redis weakens rate limiting but never takes the API down, which is also why `/readyz` does not require Redis. Cron jobs are exempt.

## Graceful shutdown

On SIGTERM/SIGINT a worker:

1. stops claiming;
2. **keeps heartbeating**, so the leases of still-running jobs don't expire during the drain;
3. waits up to `SHUTDOWN_GRACE_SECONDS` for in-flight handlers and acks them normally;
4. cancels the remaining handlers and waits up to 2 s more (a handler that still returns success is acked as success);
5. stops heartbeating and **releases** the remaining leases in one statement: `state=pending, attempts=attempts-1, run_at=now()`. An interrupted shutdown is not the job's fault, so it doesn't consume an attempt, and the job can be claimed again immediately;
6. closes the leader connection (which frees the advisory lock), flushes traces and exits 0.

The startup check `SHUTDOWN_GRACE_SECONDS + 3 < 30` guarantees the sequence fits in Compose's `stop_grace_period` and Kubernetes' `terminationGracePeriodSeconds` (30 s). A SIGKILLed worker releases nothing; its leases expire and the reaper recovers the jobs, which is exactly what the chaos test exercises.

## Observability

- **Metrics:** Prometheus on the default registry. The API exports request and submit metrics. Workers export execution, wait, retry, stale-ack and lease-loss metrics. The leader exports queue depth and the age of the oldest pending job from one `GROUP BY` query every 5 s. Depth and oldest-age series are zero-filled for every queue, so dashboards show zero rather than gaps.
- **HTTP route labels:** HTTP metrics use the chi route pattern as the `route` label. The label set stays bounded, and the same pattern names the request span.
- **Tracing:** a submit stores the W3C `traceparent` in `jobs.trace_parent`, never in the payload. The worker starts `job.execute` as a child of the submit span, and the `http` handler's client is instrumented, so a single trace shows `POST /v1/jobs` → `job.execute` → the outbound HTTP call → the downstream service.
- **Logs:** JSON logs via `slog`. Payloads, results, API keys and auth headers are never logged.

## Admin UI

The admin UI is server-rendered `html/template` plus embedded htmx: no build step and no JS framework. Each page renders either as a full document or, with `?partial=1` or on a POST, as the fragment htmx swaps in. CSRF protection comes from requiring `HX-Request: true` on every non-GET request; a cross-site form cannot set that header without a CORS preflight, which the server never grants. Basic-auth credentials are compared in constant time.

## Why the job store is not Redis

Correctness here rests on guarantees Postgres provides inside a single transaction:

- a conditional update fenced by a monotonic token;
- row locks that competing claimers can skip;
- a unique index for idempotency;
- an event row written atomically with each state change;
- one clock (`now()`) for leases and schedules.

In Redis each of these would have to be rebuilt from Lua scripts and careful key design. Durability would depend on AOF `fsync` settings, which trade throughput against the "no lost jobs" goal, and failover of a replicated Redis can drop writes that were already acknowledged. Postgres also lets the admin UI, stats, the chaos verifier and ad-hoc debugging query jobs with plain SQL.

The costs are a throughput ceiling set by write IOPS and tables that grow without bound (there is no retention yet). The measured throughput is well beyond what this system needs.

## Delivery semantics

Delivery is **at-least-once**. A job can run more than once: a worker can die after the side effect but before its ack, or a lease can expire during a long pause. Fencing guarantees that only one attempt's *outcome* is recorded, but it cannot undo side effects, so handlers must be idempotent (keyed on `X-Job-Id` or a business key). The chaos test measures the duplicate-execution rate rather than claiming it is zero.

## Scaling on Kubernetes

The manifests run a fixed number of workers. Autoscaling should use queue depth rather than CPU, for example KEDA's Prometheus scaler on `sum(queue_depth{state="pending"})` or `max(queue_oldest_pending_age_seconds)`, because workers are I/O-bound. Scaling in is safe: SIGTERM triggers the graceful release above well within the 30 s grace period.

## HTTP jobs

`http` jobs call an allowlisted host (`HTTP_HANDLER_ALLOWED_HOSTS`, exact hostnames). Each request carries `X-Job-Id`, `X-Lease-Token`, `X-Attempt` and a W3C `traceparent`, so the receiving service can join the job's trace. When `HTTP_HANDLER_QUEUE_TOKEN` is set it is sent as `X-Queue-Token`, a shared secret the receiver can check. These headers cannot be overridden from the job payload.

The response status decides the outcome: 2xx succeeds; any other 4xx fails permanently, except 408, 409 and 429, which are retried with backoff like 5xx responses and network errors. Receivers should be idempotent, keyed on `X-Job-Id` or a business key, and submitters can use an idempotency key such as `<entity>:<content hash>` so that re-submitting identical work is a no-op.
