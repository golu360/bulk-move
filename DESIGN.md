# Design

A bulk move takes a filter and a target stage, and moves every matching opportunity in one workspace to that stage. This document explains how it works, then goes deeper on each guarantee and where it stops holding.

Code: `internal/bulkmove`. Schema: `migrations/`.

## How it works

```text
Snapshot  ──▶  Lease  ──▶  Chunks  ──▶  Done
  (API)       (worker)     (worker)
```

**1. Snapshot (API).** In one transaction, the API:

- inserts the job, keyed by `Idempotency-Key`;
- copies every matching deal into `bulk_job_items` as `(seq, opportunity_id, snapshot_version)`, where `seq` runs `1..total`;
- returns `202`.

The set of deals to move is now fixed.

**2. Lease (worker).** A worker claims the oldest claimable job by writing a fresh random `lease_token` and an expiry into the job row. Only the holder of that token may write to the job. If the worker dies, the lease expires and another worker takes over.

**3. Chunks (worker).** The worker repeats until done:

- Read the next 500 items after `cursor_seq`.
- Move the deals that are unchanged since the snapshot.
- Advance the cursor and renew the lease.

Each chunk is one transaction, so a crash loses at most the chunk in flight, and the next owner redoes it from the cursor.

Separate processes and pools: `cmd/api` has 20 connections, and `cmd/worker` runs N claim loops on its own pool of N+1. Postgres is shared.

---

## 1. Snapshot vs live filter

The deals are fixed at submit (`TestSubmitSnapshotIsFixed`). The filter is never re-run.

**Why:**

- `total` is known up front. That makes progress accurate and lets the 50,000 cap reject a request (`422 too_many_matches`) before any work is done.
- Each chunk is a cheap range read, not a filter query against a table that is changing underneath it.
- `snapshot_version` gives an exact per-deal conflict check. A live filter can't tell "a user moved this deal" from "this deal now matches".

**Costs:**

- Deals created after submit are not moved.
- **Any** write after submit excludes a deal, even a rename. This is conservative on purpose: a skipped deal can be moved again, but a lost user edit can't be recovered. `skipped` reports how many were excluded.
- The snapshot runs synchronously in the HTTP request: up to 50,000 inserted rows, on an API connection, with a 10 s timeout.
- Item rows are never deleted.

## 2. Idempotency

`Idempotency-Key` is required: 1–200 printable ASCII characters. It is unique per workspace and never expires. It is stored with `request_hash`, a SHA-256 of the normalized request (times in UTC, field order and whitespace ignored). `INSERT … ON CONFLICT DO NOTHING` decides the outcome:

| Case | Result |
|---|---|
| New key | Job created, `202` |
| Same key, same request | Same job with its current status, `202`, `X-Idempotency-Replayed: true`. A replay never restarts a job. |
| Same key, different request | `422 idempotency_key_reused`. The original job is untouched. |
| Concurrent identical submits | The second insert waits on the unique index, then returns the winner's job. 50 parallel submits create 1 job. |
| Over the cap | Everything rolls back, so the key is not used up. |

The worker needs no key. Each chunk is atomic and records its own cursor position.

**Where duplicates still get through:**

- **A retry with a new key creates a second job.** Usually that is harmless, because the deals are already in the target stage or their versions have moved on (`TestOverlappingJobsSecondSkipsMovedDeals`). It does harm if users moved deals *out* of the target stage between the two submits: the second job moves them back.
- **Manual moves have no key and no expected version.** A late retry overwrites a newer change.
- **The hash compares how a request is written, not what it selects.** Two filters that select the same deals but are written differently count as different requests, and the second gets `422`. This fails safe.

## 3. Leasing, chunking and recovery

**A chunk is a primary-key range.** It is the items with `seq ∈ (cursor_seq, cursor_seq + 500]` on the PK `(job_id, seq)`. Item 49,500 costs the same as item 1: there is no `OFFSET` and no re-filtering.

**One chunk is one transaction** (`ApplyChunk`):

1. **Fence:** `SELECT … FOR UPDATE` on the job row where `lease_token = @token`. If no row comes back, the lease was lost (`ErrLeaseLost`) and the worker stops without writing.
2. Lock, in ID order, the deals that are unchanged since the snapshot and not already in the target stage.
3. Update those deals, bump their `version`, and insert one `bulk` transition per deal.
4. Advance `cursor_seq`, add to `moved` and `skipped`, and renew the lease. On the last chunk, mark the job `completed`.

**Why it is crash-safe:**

- The cursor is written only by the transaction that does the work it counts. After a crash, the chunk in flight rolls back and the cursor still points before it, so the next owner redoes that chunk exactly once.
- As a backstop, the unique index `(bulk_job_id, opportunity_id)` on transitions makes double-recording a deal impossible, even if this logic has a bug.

**How the lease and the row lock divide the work:**

- **Between chunks, the lease protects ownership.** A committed chunk renews it, and there is no separate heartbeat.
- **During a chunk, the row lock does.** The fence's row lock lasts until commit. `Claim` uses `SKIP LOCKED`, so it can't take a job mid-chunk, even if the lease has expired.
- `Config.Validate` requires `LeaseTTL > LockTimeout + ChunkPause`, so a healthy worker never loses its lease.

**When something goes wrong:**

| Event | What happens | Counts as a failed attempt? |
|---|---|---|
| Chunk errors (DB error, lock timeout) | Rolled back. The job is released with a backoff, and any worker may retry it afterwards. | Yes |
| Worker dies | The lease expires, and the next `Claim` takes over with a new token. | Yes |
| Old worker wakes up | Its next fence finds no row (`ErrLeaseLost`), and it stops without writing. | — |
| Graceful shutdown | Rolled back. The job is released immediately. | No |
| A chunk commits | `attempts` resets to 0. | — |

After 5 failed attempts in a row with no chunk committing in between (`MaxChunkFailures`), the job is marked `failed`. A chunk that kills its worker every time therefore fails the job instead of retrying forever.

**Indexes:**

| Index | Used by |
|---|---|
| `bulk_job_items` PK `(job_id, seq)` | every chunk read |
| `opportunities` PK | the item → deal join |
| `opportunities (workspace_id, stage_id, id)`, `(workspace_id, owner_id, id)`, `(workspace_id, created_at)` | snapshot scans for the common filters |
| `bulk_jobs_claimable (created_at) WHERE status IN ('pending','running')` | `Claim`, whose cost stays flat as finished jobs pile up |

## 4. Concurrency on a single deal

**The rule:** every write to `opportunities` bumps `version`. The job moves a deal only if its version still matches the snapshot. **A user's write is never overwritten by the job.**

Both sides lock the row and cap how long they wait for the lock:

- A manual move waits up to 2 s, then the API returns `503`.
- A chunk waits up to 5 s, then it is retried. It locks in ID order, so two chunks can't deadlock.

| Race | Outcome |
|---|---|
| User edits first | The version no longer matches, so the chunk skips the deal. The user wins. |
| User holds the row while the chunk waits | When the user commits, `READ COMMITTED` re-checks the chunk's `WHERE` against the new row. That one deal drops out and the rest of the chunk continues (`TestUserEditDuringChunkWins`). Under `REPEATABLE READ`, this wait would abort all 500 rows. |
| Chunk holds the row while the user waits | The user waits milliseconds, then moves the deal again on top of the bulk move. Both transitions are recorded (`TestUserMoveAfterJobMovedDealApplies`). |

## 5. Isolation, and its holes

**From interactive traffic:**

- The worker has its own connection pool.
- Chunks are 500 rows with a 50 ms pause between them, so row locks last milliseconds.
- Lock waits are bounded on both sides.
- Measured at 200 req/s of mixed traffic during a 50,000-deal move, the p99 for list-stage and move-deal stayed within baseline (see `BENCHMARKS.md`).

**Between workspaces:**

- **At most one running job per workspace, enforced by the database.** The partial unique index `bulk_jobs_one_running` decides. When two workers race, the loser gets `23505`, which `Claim` treats as nothing to claim.
- **A busy workspace can't starve the others.** It holds at most one worker slot (`TestBusyWorkspaceDoesNotStarveOthers`).
- **Every query includes `workspace_id`,** including inside the chunk, so even a corrupted snapshot can't cross tenants.

**Holes:**

- **Postgres is shared.** Separate pools isolate connections, not CPU, I/O, WAL or vacuum.
- **Submit isn't isolated.** A burst of large snapshots can use up the API pool.
- **A stalled worker holds its locks indefinitely.** `Claim` skips locked rows, and `idle_in_transaction_session_timeout` is not set on the worker pool. Manual moves on those deals return `503` until the connection dies.
- **Slots are first come, first served.** There are 4 of them, with no weighting by job size or tenant.
- **A job in retry backoff keeps its workspace blocked** until it succeeds or reaches `MaxChunkFailures`.

## 6. What breaks at 10×

The case: a 500,000-deal move in a workspace of 20M opportunities.

1. **The 50,000 cap rejects it, by design.** The client can split it into 10 moves, which run one after another with the same guarantees.
2. **If the cap is raised, write volume is the limit.** The snapshot and the chunks still use indexes, but each move is expensive:
   - `stage_id` is indexed, so updates can't be HOT. Every move writes a new tuple plus four index entries and a transition row.
   - The updates are scattered across 20M rows, so each checkpoint triggers full-page writes.
   - 500k moves add up to gigabytes of WAL in minutes, which shows up as disk contention and replica lag.

   *Fix:* pace chunks by a write budget (WAL rate or replica lag) instead of a fixed pause.
3. **The workspace is blocked for the whole run.** This needs a cancel endpoint and, eventually, priorities.
4. **The synchronous snapshot can time out.** With no index matching the filter and a cold cache, the request scans 20M rows. *Fix:* accept the job immediately and let the worker build the snapshot in pages.
5. **Nothing is deleted.** Items, transitions and keys grow forever. *Fix:* purge items when a job finishes, expire keys, and partition transitions by time.
6. **Bloat outruns vacuum.** Each large move leaves many dead tuples but stays under autovacuum's default threshold. *Fix:* per-table autovacuum settings.
