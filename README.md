# pgqueue

A Postgres-backed distributed job queue written in Go. Clients submit jobs over HTTP. Horizontally scaled workers claim them with `FOR UPDATE SKIP LOCKED` and run them under leases with heartbeats and fencing tokens. Failed jobs are retried with jittered exponential backoff until they reach a replayable dead state. One elected leader runs the reaper and the cron scheduler. Every part is observable through Prometheus, Grafana and OpenTelemetry/Jaeger, and a chaos test kills workers under load to prove that no job is lost.

Design notes live in [ARCHITECTURE.md](ARCHITECTURE.md).

## Architecture

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

## Quickstart

```bash
cp .env.example .env
# edit .env: set ADMIN_PASSWORD=<something>
make up          # builds and starts api + 3 workers + observability

curl -s -X POST localhost:8081/v1/jobs \
  -H 'Authorization: Bearer demo-key-change-me' \
  -d '{"queue":"default","type":"sleep","payload":{"ms":500}}'
# → 201 {"job":{"id":"<id>","state":"pending",...}}

curl -s localhost:8081/v1/jobs/<id> -H 'Authorization: Bearer demo-key-change-me'
# → 200 {"job":{...,"state":"succeeded","result":{"slept_ms":500}},"events":[...]}
```

| UI | URL |
|---|---|
| Admin UI (basic auth `admin` / your `ADMIN_PASSWORD`) | http://localhost:8081/admin |
| Grafana (anonymous viewer, dashboard "Job Queue") | http://localhost:3001 |
| Jaeger | http://localhost:16687 |
| Prometheus | http://localhost:9092 |

Postgres is published on `localhost:5433` for `loadgen verify` and debugging.

## API reference

All `/v1` routes require `Authorization: Bearer <key>`. Errors use `{"error":{"code","message","index"?}}`.

| Method & path | Request | Success | Errors |
|---|---|---|---|
| `POST /v1/jobs` | `{queue, type, payload, priority?, max_attempts?, timeout_seconds?, run_at? \| delay_seconds?, idempotency_key?}` | 201 `{"job"}` created, 200 on idempotent hit | 400, 401, 413, 429 |
| `POST /v1/jobs/batch` | `{"jobs":[1..500 submit objects]}` | 201 if any created, else 200: `{"results":[{"job","created"}]}` | 400 (with `index`; nothing inserted), 401, 413, 429 |
| `GET /v1/jobs/{id}` | – | 200 `{"job","events"}` (last 20, newest first) | 401, 404 |
| `GET /v1/jobs` | `state, queue, type, limit (≤100), cursor` | 200 `{"jobs","next_cursor"}` | 400, 401 |
| `POST /v1/jobs/{id}/cancel` | – | 200 (pending/cancelled) or 202 (running: cancel requested) | 401, 404, 409 |
| `POST /v1/jobs/{id}/replay` | – | 200 (dead → pending, attempts reset) | 401, 404, 409 |
| `POST /v1/cron` | `{name, expr, queue, type, payload, enabled?}` | 201 `{"schedule"}` | 400, 401, 409 |
| `GET /v1/cron` | – | 200 `{"schedules"}` | 401 |
| `PATCH /v1/cron/{id}` | `{enabled}` | 200 `{"schedule"}` | 400, 401, 404 |
| `DELETE /v1/cron/{id}` | – | 204 | 401, 404 |
| `GET /v1/stats` | – | 200 `{"by_queue":{q:{scheduled,pending,running,succeeded,dead,cancelled}}}` | 401 |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | – | liveness, readiness (Postgres required, Redis reported), Prometheus | – |

Job types: `http` (allowlisted hosts only), `noop`, `sleep {ms}`, `fail {rate}`, `record {ms?}`. Cron expressions are 5-field or descriptors (`@hourly`, `@every 5m`), evaluated in UTC. Time-zone prefixes and `@every` intervals under 1 minute are rejected.

## Delivery semantics

Delivery is **at-least-once**, never exactly-once.

- Each claim issues a new, globally increasing **fencing token** (`lease_token`). Every completion write is conditioned on `lease_token = <mine> AND state = 'running'`. A worker whose lease expired (it stalled, lost the network, or was killed and reclaimed) gets `ErrLeaseLost` and cannot overwrite a newer attempt.
- Fencing protects the job's *recorded outcome*, not your side effects. A worker can perform the side effect and die before its ack, and the job then runs again. **Handlers must be idempotent**, for example keyed on the `X-Job-Id` header or on a business key. The chaos test measures how often duplicates actually happen.

## Benchmarks

`make bench` scales the workers to 1, 2, 4, 8 and 16 and pushes 20 000 `sleep 50ms` jobs through each configuration as fast as the load generator can submit them (`-rate 0`), with tracing sampled at 1%. Each worker runs 16 jobs at a time, so one worker can complete at most 320 jobs/s (19 200/min). The results are written to `bench/results/scaling.md`:

| run_id | workers | worker_concurrency | n | type | submit_jobs_per_s | complete_jobs_per_min | wait_p50_ms | wait_p95_ms | wait_p99_ms | e2e_p50_ms | e2e_p95_ms | e2e_p99_ms | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1790513287 | 1 | 16 | 20000 | sleep | 5526 | 18106 | 30139 | 58914 | 61528 | 30190 | 58965 | 61578 | 0 |
| 1790513365 | 2 | 16 | 20000 | sleep | 5297 | 35757 | 14955 | 27644 | 28946 | 15006 | 27695 | 28997 | 0 |
| 1790513411 | 4 | 16 | 20000 | sleep | 4525 | 67450 | 6312 | 11710 | 12177 | 6363 | 11761 | 12228 | 0 |
| 1790513441 | 8 | 16 | 20000 | sleep | 3612 | 123497 | 2531 | 3828 | 3917 | 2585 | 3879 | 3967 | 0 |
| 1790513463 | 16 | 16 | 20000 | sleep | 2402 | 127097 | 56 | 121 | 139 | 119 | 185 | 212 | 0 |

Throughput scales almost linearly up to 8 workers: 18.1k, 35.8k, 67.5k and 123.5k jobs/min, which is 94% down to 80% of the ideal. From 8 to 16 workers it goes flat at about 127k jobs/min. At 16 workers the queue keeps up with arrivals (median wait 56 ms), and the limit is the single 4-CPU Docker VM running Postgres, the API and all workers, where job inserts and claims compete for the same database. On separate machines the ceiling moves to Postgres write throughput.

## Chaos test

`make chaos` pushes 20 000 `record` jobs (max 25 attempts, 2 000 jobs/s) while SIGKILLing a random worker container every 10 s for 3 minutes. It then checks Postgres directly. The run fails if any job did not succeed, or if a succeeded job has no recorded execution for its final lease.

| total | lost | acked_execution_missing | duplicate rate | reclaim p50 | reclaim p95 | reclaim max | kills |
|---|---|---|---|---|---|---|---|
| 20000 | 0 | 0 | 0.24% (48 jobs) | 5.17 s | 5.47 s | 5.64 s | 15 |

Duplicates are expected under at-least-once delivery: they are jobs whose worker was killed after the side effect but before the ack. Reclaim time is measured from lease expiry to the next claim, so it is bounded by `REAPER_INTERVAL_SECONDS` plus the retry backoff.

## Running on kind

```bash
make kind-up      # kind cluster "jq", builds and loads images, applies deploy/k8s/
kubectl -n jobqueue get pods -w
kubectl -n jobqueue port-forward svc/api 8080:8080
curl -s -X POST localhost:8080/v1/jobs -H 'Authorization: Bearer demo-key-change-me' \
  -d '{"queue":"default","type":"noop","payload":{}}'
make kind-down
```

The Secret in `deploy/k8s/secret.yaml` holds placeholder values for local use. Replace them before any shared deployment.

## Development

```bash
make test-unit    # unit tests only (-short)
make test         # unit + integration (testcontainers: needs Docker)
make lint         # go vet + staticcheck
```

## Admin UI

`/admin` (basic auth) shows live queue × state counts, which refresh every 2 s, along with the current scheduler leader and a pause/resume button per queue. The job list filters by state, queue, type and tenant. Dead jobs can be replayed one at a time or in bulk. The job detail page shows the payload, result, last error and event history, with cancel and replay buttons.

## Limitations

Deliberately not supported:

- Redis is not a job store (it is used only for rate limiting).
- No job dependencies, per-queue concurrency limits, or per-tenant fairness or priority aging. Queues can starve each other under global priority order.
- No autoscaling/HPA (queue-depth-based scaling is described in ARCHITECTURE.md), gRPC, or exactly-once semantics.
- **No job retention or cleanup: the tables grow without bound.**
- No tenant or API-key CRUD, and no queue-creation API (queues come from the migration).
- No payload encryption, TLS, or DB replication.
- No job progress or log streaming, completion webhooks, or editing a job after submit.
- No OpenAPI generation or Helm charts.
- `/metrics` is unauthenticated; expose it on internal networks only.
- Only the registered handler types run; there is no arbitrary user code.
