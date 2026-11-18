CREATE TABLE bulk_jobs (
  id               bigserial   PRIMARY KEY,
  workspace_id     bigint      NOT NULL REFERENCES workspaces(id),
  idempotency_key  text        NOT NULL,
  request_hash     bytea       NOT NULL,
  filter           jsonb       NOT NULL,
  target_stage_id  bigint      NOT NULL REFERENCES stages(id),
  status           text        NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'completed', 'failed')),

  -- Progress. Only ever updated in the same transaction as the chunk it counts.
  total            int         NOT NULL DEFAULT 0,
  cursor_seq       int         NOT NULL DEFAULT 0,   -- last item processed
  moved            int         NOT NULL DEFAULT 0,
  skipped          int         NOT NULL DEFAULT 0,   -- edited since submit, already in target, or deleted
  last_progress_at timestamptz,

  -- Lease. New random token per claim; only the token holder may write.
  lease_token      text,
  lease_expires_at timestamptz,
  claims           int         NOT NULL DEFAULT 0,   -- > 1 means the job was resumed
  last_error       text,

  created_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz
);

-- Idempotency: one job per key per workspace, forever (keys never expire; documented).
CREATE UNIQUE INDEX bulk_jobs_idem ON bulk_jobs (workspace_id, idempotency_key);

-- Isolation: at most one running job per workspace, enforced by the database.
CREATE UNIQUE INDEX bulk_jobs_one_running ON bulk_jobs (workspace_id) WHERE status = 'running';

-- Keeps the claim query cheap as finished jobs accumulate.
CREATE INDEX bulk_jobs_claimable ON bulk_jobs (created_at) WHERE status IN ('pending', 'running');

-- The snapshot taken at submit. seq runs 1..total, so a chunk is a PK range scan.
CREATE TABLE bulk_job_items (
  job_id           bigint NOT NULL REFERENCES bulk_jobs(id) ON DELETE CASCADE,
  seq              int    NOT NULL,
  opportunity_id   bigint NOT NULL,
  snapshot_version int    NOT NULL,
  PRIMARY KEY (job_id, seq)
);

-- Backstop: a job can never record two transitions for the same deal.
CREATE UNIQUE INDEX stage_transitions_bulk_once
  ON stage_transitions (bulk_job_id, opportunity_id) WHERE bulk_job_id IS NOT NULL;
