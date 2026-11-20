# Design

A bulk move takes a filter and a target stage, and moves every matching opportunity in one workspace to that stage. The API accepts the request and records a job. A separate worker process runs the job in chunks. This document covers how that stays correct across crashes, retries and concurrent edits, and where it stops being correct.

Components: `cmd/api` (HTTP, 20-connection pool), `cmd/worker` (N claim loops, its own pool of N+1 connections), and one Postgres database. The code is in `internal/bulkmove`, and the schema is in `migrations/`.

## 1. Chunking and the cursor

**Snapshot at submit.** `POST /bulk-moves` inserts the job and its items in one transaction. Each item is `(job_id, seq, opportunity_id, snapshot_version)`, and `seq` is a dense `1..total` assigned by `row_number() OVER (ORDER BY id)`.

**A chunk is a primary-key range.** Chunk *k* is the items with `seq ∈ (cursor_seq, cursor_seq + ChunkSize]`. The table's primary key is `(job_id, seq)`, so reading a chunk is a single index range scan. It costs the same at item 49,500 as at item 1: there is no `OFFSET`, and no re-filtering of the opportunities table. Each item is joined to `opportunities` by primary key. Because `seq` is dense, every chunk except the last contains exactly `ChunkSize` items.

**One chunk = one transaction** (`ApplyChunk`). Each chunk does the following, in order:

1. Fence on the lease token, and lock the job row.
2. Lock, in ID order, the deals that are unchanged since the snapshot and not already in the target stage.
3. Update those deals and bump their `version`.
4. Insert one `bulk` stage transition per moved deal.
5. Advance `cursor_seq`, add to `moved` and `skipped`, and renew the lease.
6. On the last chunk, mark the job `completed`.

All of it commits together or not at all.

**Restart.** The cursor lives in `bulk_jobs.cursor_seq` and is written only inside the chunk transaction that does the work it counts. After a crash, the uncommitted chunk has rolled back and the cursor still points before it. Whoever claims the job next resumes from `cursor_seq` and redoes that chunk exactly once. The unique index `stage_transitions_bulk_once (bulk_job_id, opportunity_id)` is a backstop: even a bug in this logic cannot record a deal twice for the same job.

**Ownership is a lease.** `Claim` sets two things on the job row:
- `lease_token`, a fresh random value for every claim
- `lease_expires_at`, set to `now() + LeaseTTL`

Every write is fenced with `WHERE lease_token = @token`. A committed chunk renews the lease, and there is no separate heartbeat. A worker that dies stops renewing. After `LeaseTTL` another worker takes the job over with a new token. If the old worker wakes up, its next write fails the fence (`ErrLeaseLost`) and it stops without writing anything.

`Config.Validate` requires `LeaseTTL > LockTimeout + ChunkPause`. This ensures a healthy chunk that is waiting on a lock can never lose its lease.

**Failures.**
- A chunk that errors releases the job: the token is set to NULL, `attempts` goes up by 1, and the lease is set to `now() + RetryBackoff`. Any worker may retry it once the backoff is over.
- A takeover from a dead worker also counts as an attempt.
- A graceful shutdown (SIGTERM) is not a failure. The in-flight chunk rolls back, and the worker sets the token to NULL and the lease to `now()` without touching `attempts`. Any worker may resume the job at once. If that release fails, the job resumes after `LeaseTTL`, as for a dead worker.
- Committed progress resets `attempts` to 0.
- At `MaxChunkFailures` failed attempts without progress, the job is marked `failed`. A chunk that kills its worker every time therefore ends as a failed job and is not resumed forever.

**Indexes that keep this cheap:**

| Index | Serves |
|---|---|
| `bulk_job_items` PK `(job_id, seq)` | every chunk read |
| `opportunities` PK | the item → deal join |
| `opportunities (workspace_id, stage_id, id)`, `(workspace_id, owner_id, id)`, `(workspace_id, created_at)` | the snapshot scan for the common filters |
| `bulk_jobs_claimable (created_at) WHERE status IN ('pending','running')` | `Claim` cost stays flat as finished jobs pile up |

## 2. Idempotency

**The key** is the `Idempotency-Key` header. It is required and must be 1–200 printable ASCII characters.

**Where it is stored.** It is stored in `bulk_jobs.idempotency_key`, together with `request_hash`, which is the SHA-256 of the decoded and normalized request. Normalization means times are converted to UTC, and field order and whitespace don't matter. The key is unique per `(workspace_id, idempotency_key)` and never expires.

**How a submit uses it.** The submit runs `INSERT … ON CONFLICT DO NOTHING`, and the unique index settles the outcome:

- **New key:** the job and its snapshot are created, and the API returns `202`.
- **Same key, same request:** the original job is returned with its *current* status, a `202`, and `X-Idempotency-Replayed: true`. A replay never restarts a job. To run the same move again, the client uses a new key.
- **Same key, different request:** the API returns `422 idempotency_key_reused`, and the original job is untouched.
- **Concurrent identical submits:** the second `INSERT` blocks on the index until the first transaction ends, then reads the winner's job. 50 parallel identical requests create one job (`TestIdem50ConcurrentIdenticalRequests`).
- **Over the cap:** the job and its items roll back together, so the key is not consumed.

**What it protects.** It protects against duplicate jobs from retries, lost responses and double-clicks. The worker side doesn't need a key, because each chunk is atomic and records its own cursor position.

**Where a retry still slips through:**

- **The client retries with a new key.** This creates a second job. Usually that is harmless: the second job's snapshot either sees the deals already in the target stage, or holds versions that the first job then bumps, so it skips them (`TestOverlappingJobsSecondSkipsMovedDeals`). It does real harm if, between the two submits, users moved deals out of the target stage. The second job's snapshot then captures their new versions and moves those deals back.
- **Manual moves have no key and no expected version.** A retried `POST /opportunities/{id}/move` that arrives after someone else moved the deal silently overwrites that change. A request that sends the expected version and gets `409` on a mismatch would close this.
- **The hash compares requests by their encoded form, not their meaning.** Two filters that select the same deals but are written differently hash differently, so the second is rejected as key reuse. This fails safe: it rejects the request rather than running a duplicate.
- **Keys never expire.** This is stated in the API contract. It also means unbounded growth, as covered in §6.

## 3. Concurrency on a single opportunity

Every write to `opportunities` bumps `version`. This is the one rule every future write path must follow, and the bulk job's conflict check relies on it.

Both writers take a row lock, and both cap how long they will wait for one:
- A manual move does `SELECT … FOR UPDATE` with `lock_timeout = 2s`. If the timeout is hit, the API returns `503 busy`.
- A chunk does `FOR UPDATE OF o` (in ID order, so two chunks cannot deadlock) with `lock_timeout = 5s`.

**The user edits before the chunk reaches the deal.** The version no longer matches the snapshot, so the chunk skips the deal and counts it in `skipped`. **The user's edit wins.**

**The user's transaction holds the row while the chunk tries to lock it.** The chunk waits. It runs at `READ COMMITTED` on purpose:
- After the user commits, Postgres re-checks the chunk's `WHERE` clause against the new row version. The version has changed, so that one row drops out and the rest of the chunk proceeds (`TestUserEditDuringChunkWins`).
- Under `REPEATABLE READ`, the same wait would abort the whole 500-row chunk with a serialization failure.
- If the user holds the row longer than 5s, the chunk rolls back and is retried after the backoff.

**The chunk holds the row while the user tries to move it.** The user waits for the chunk to commit, which takes milliseconds. The user's move then applies on top of the bulk move. The last write wins, and both transitions are recorded (`TestUserMoveAfterJobMovedDealApplies`). If a chunk ever held its locks longer than 2s, the user would get `503` and could retry.

**Net policy:** a deal is moved by the job only if nobody has written to it since submit. A user write is never overwritten by the job.

## 4. Snapshot vs live filter

**The choice:** the set of deals is fixed at submit time (`TestSubmitSnapshotIsFixed`). The job does not re-evaluate the filter as it runs.

**Benefits:**
- `total` is known when the job is accepted, so progress is accurate and the 50,000 cap is enforced before any work is done (`422 too_many_matches`).
- The chunk is a cheap range read on `bulk_job_items`, not a repeated filter query on a table that is changing underneath it.
- The snapshot version gives a precise, per-deal conflict check. A live filter can't tell "the user moved this deal" from "this deal now matches".

**Consequences:**
- **Deals created after submit are not moved,** even if they match the filter.
- **Any write to a deal after submit excludes it,** even one unrelated to the filter, such as a rename. This deliberately leans conservative: a skipped deal can be moved again, but a lost user edit can't be recovered. `skipped` reports how many were excluded.
- **The submit does the snapshot work synchronously.** Up to 50,001 rows are scanned and 50,000 inserted inside the HTTP request, with a 10s timeout, on an API connection.
- **Storage.** Every job keeps one row per item, and nothing deletes them yet.

## 5. Isolation, and the holes left

**Between the bulk job and interactive traffic:**

- **Separate pools.** The worker uses its own pool of `Workers + 1` connections, so bulk work can never take an API connection.
- **Short transactions.** Chunks are 500 rows, with a 50ms pause between them, so row locks are held for milliseconds.
- **Bounded lock waits.** Each side caps how long it waits for the other: 2s for the user and 5s for the chunk.
- **Benchmark results.** Measured at 200 req/s of mixed traffic, list-stage and move-deal p99 stayed within baseline during a 50,000-deal move (2.6 ms / 9.8 ms; see `BENCHMARKS.md`).

**Between workspaces:**

- **One running job per workspace, enforced by the database.** The partial unique index `bulk_jobs_one_running ON (workspace_id) WHERE status = 'running'` is the actual rule. The `NOT EXISTS` in `Claim` only filters candidates. Two workers that race to start jobs in the same workspace both pass it, and the loser gets `23505` and treats it as "nothing to claim".
- **No workspace can starve the others.** A workspace with a queue of jobs occupies at most one worker slot, so another workspace's later job is claimed past the queue (`TestBusyWorkspaceDoesNotStarveOthers`).
- **Every query includes `workspace_id`.** The chunk re-checks `o.workspace_id`, so even a corrupted snapshot can't cross tenants. Another workspace's job returns `404`.

**Holes:**

- **The database is shared.** Separate pools isolate connections, not CPU, I/O, WAL or vacuum. A heavy job still competes with interactive queries inside Postgres, and we have measured that only on one machine.
- **Submit is not isolated.** The snapshot runs on the API pool for up to 10s, so a burst of large submits can use up API connections.
- **A worker stalled mid-chunk holds its locks indefinitely.** Its transaction holds the job row and up to 500 deal locks. `Claim` skips locked rows, so the lease cannot rescue the job. Manual moves on those deals return `503` until the connection dies. `idle_in_transaction_session_timeout` is not set on the worker pool, and setting it would bound this.
- **Slots are first come, first served.** There are 4 slots in total, assigned oldest-first. If more than 4 workspaces have long jobs, the rest wait for a slot. Nothing weights by size or tenant.
- **A failed chunk keeps its workspace blocked.** A job in retry backoff stays `running`, so its workspace's other jobs wait. This is deliberate, but a job that keeps failing blocks the workspace until it reaches `MaxChunkFailures`.

## 6. What breaks at 10×

The case: a 500,000-deal move in a workspace that holds 20M opportunities.

**First to fail: the 50,000 cap, by design.** The move is rejected with `422 too_many_matches` before any work starts. The client can split it into ten narrower moves with ten keys, which run one after another with the same guarantees.

**If the cap is raised, the code path holds up, but write volume becomes the limit:**
- The snapshot is still an index scan.
- Chunks are still primary-key range reads.
- Each move costs far more than its row, though:
  - `stage_id` is indexed, so no update can be HOT. Every move writes a new heap tuple plus an entry in all four indexes.
  - Each move also writes a transition row.
  - The updates are scattered across 20M rows, causing full-page writes after every checkpoint.
- A 500k move becomes gigabytes of WAL in a minute or two. That competes with interactive traffic for disk, and shows up as replica lag.

*Fix:* pace chunks by a write budget (replica lag or WAL rate) instead of a fixed pause, and set the cap from that budget.

**Then, in order:**
- **The workspace is blocked for the whole run.** Only one job runs per workspace, so its other bulk moves queue behind it. This needs a cancel endpoint and, eventually, priorities.
- **The synchronous snapshot gets risky.** A filter with no matching index scans the entire 20M-row workspace inside the 10 s request. With a cold cache, that can time out. *Fix:* accept the job immediately and let the worker build the snapshot in keyset pages.
- **Nothing is ever deleted.** Snapshot items, transitions and idempotency keys grow with every job. *Fix:* purge items when a job finishes, give keys a time limit, and partition transitions by time.
- **Bloat outruns vacuum.** Each large move leaves hundreds of thousands of dead tuples but stays under autovacuum's default threshold. Index bloat builds up across moves until the table gets its own autovacuum settings.
