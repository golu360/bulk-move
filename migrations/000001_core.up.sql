CREATE TABLE workspaces (
  id   bigserial PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE stages (
  id           bigserial PRIMARY KEY,
  workspace_id bigint NOT NULL REFERENCES workspaces(id),
  position     int    NOT NULL,
  name         text   NOT NULL,
  UNIQUE (workspace_id, position)
);

CREATE TABLE opportunities (
  id           bigserial   PRIMARY KEY,
  workspace_id bigint      NOT NULL REFERENCES workspaces(id),
  stage_id     bigint      NOT NULL REFERENCES stages(id),
  name         text        NOT NULL,
  value_cents  bigint      NOT NULL CHECK (value_cents >= 0),
  status       text        NOT NULL DEFAULT 'open'
               CHECK (status IN ('open', 'won', 'lost', 'abandoned')),
  owner_id     bigint      NOT NULL,
  version      int         NOT NULL DEFAULT 0,  -- bumped on EVERY write; the bulk job's conflict check
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);
-- Stage listing with keyset pagination; also the snapshot scan for stage filters.
CREATE INDEX opportunities_ws_stage_id ON opportunities (workspace_id, stage_id, id);
CREATE INDEX opportunities_ws_owner    ON opportunities (workspace_id, owner_id, id);
CREATE INDEX opportunities_ws_created  ON opportunities (workspace_id, created_at);

CREATE TABLE stage_transitions (
  id             bigserial   PRIMARY KEY,
  workspace_id   bigint      NOT NULL,
  opportunity_id bigint      NOT NULL REFERENCES opportunities(id),
  from_stage_id  bigint      NOT NULL,
  to_stage_id    bigint      NOT NULL,
  source         text        NOT NULL CHECK (source IN ('manual', 'bulk')),
  bulk_job_id    bigint,
  created_at     timestamptz NOT NULL DEFAULT now()
);
