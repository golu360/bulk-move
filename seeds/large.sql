-- One large workspace: 500,000 opportunities across 12 stages, skewed toward
-- early stages, created over the last ~18 months.
--
--   make seed-large
BEGIN;

TRUNCATE stage_transitions, bulk_job_items, bulk_jobs, opportunities, stages, workspaces RESTART IDENTITY;

SELECT setseed(0.42);  -- same data on every run

DO $$
DECLARE
  ws bigint;
BEGIN
  INSERT INTO workspaces (name) VALUES ('Large Workspace') RETURNING id INTO ws;

  INSERT INTO stages (workspace_id, position, name)
  SELECT ws, s.pos - 1, s.name
  FROM unnest(ARRAY[
    'New Lead', 'Contacted', 'Qualified', 'Discovery', 'Demo Scheduled', 'Demo Done',
    'Proposal Sent', 'Negotiation', 'Contract Sent', 'Verbal Commit', 'Closed Won', 'Closed Lost'
  ]) WITH ORDINALITY AS s(name, pos);

  -- floor(12 * random()^2) is skewed toward 0: ~29% in "New Lead", ~2% in "Closed Lost".
  WITH o AS MATERIALIZED (
    SELECT n,
           floor(12 * random() ^ 2)::int                AS pos,
           now() - random() * interval '18 months'      AS created_at
    FROM generate_series(1, 500000) n
  )
  INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, status, owner_id, created_at, updated_at)
  SELECT ws,
         st.id,
         'Large deal #' || o.n,
         (50000 + 49950000 * random() ^ 3)::bigint,    -- $500 .. $500k, mostly small
         CASE st.position WHEN 10 THEN 'won' WHEN 11 THEN 'lost' ELSE 'open' END,
         1 + floor(random() * 40)::int,
         o.created_at,
         o.created_at + random() * (now() - o.created_at)
  FROM o
  JOIN stages st ON st.workspace_id = ws AND st.position = o.pos;
END $$;

COMMIT;

ANALYZE workspaces, stages, opportunities;
