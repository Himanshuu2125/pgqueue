CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SEQUENCE lease_seq;

CREATE TABLE tenants (
  id text PRIMARY KEY,
  api_key_hash bytea NOT NULL UNIQUE,
  rate_limit_per_sec int NOT NULL,
  burst int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE queues (
  name text PRIMARY KEY CHECK (name ~ '^[a-z0-9_-]{1,64}$'),
  paused boolean NOT NULL DEFAULT false);
INSERT INTO queues(name) VALUES ('default');

CREATE TABLE cron_schedules (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL REFERENCES tenants(id),
  name text NOT NULL,
  expr text NOT NULL,
  queue text NOT NULL REFERENCES queues(name),
  type text NOT NULL,
  payload jsonb NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  last_run_at timestamptz,
  next_run_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name));

CREATE TABLE jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id text NOT NULL REFERENCES tenants(id),
  queue text NOT NULL REFERENCES queues(name),
  type text NOT NULL,
  payload jsonb NOT NULL,
  priority smallint NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
  state text NOT NULL CHECK (state IN ('pending','running','succeeded','dead','cancelled')),
  attempts int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL CHECK (max_attempts BETWEEN 1 AND 25),
  timeout_seconds int NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 3600),
  run_at timestamptz NOT NULL DEFAULT now(),
  idempotency_key text,
  lease_owner text,
  lease_expires_at timestamptz,
  lease_token bigint,
  cancel_requested boolean NOT NULL DEFAULT false,
  last_error text,
  result jsonb,
  cron_id uuid REFERENCES cron_schedules(id) ON DELETE SET NULL,
  trace_parent text,
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  finished_at timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key));

CREATE INDEX jobs_claim          ON jobs (priority DESC, run_at) WHERE state = 'pending';
CREATE INDEX jobs_running_lease  ON jobs (lease_expires_at)     WHERE state = 'running';
CREATE INDEX jobs_tenant_created ON jobs (tenant_id, created_at DESC, id DESC);

CREATE TABLE job_events (
  id bigserial PRIMARY KEY,
  job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  event text NOT NULL,
  attempt int NOT NULL,
  worker_id text,
  detail jsonb,
  created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX job_events_job ON job_events (job_id, id DESC);

CREATE TABLE executions (
  id bigserial PRIMARY KEY,
  job_id uuid NOT NULL,
  lease_token bigint NOT NULL,
  worker_id text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (job_id, lease_token));
