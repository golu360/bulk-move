# bulk-move

## Run it

Needs only Docker.

```sh
docker compose run --build --rm tests
```

This starts Postgres, runs the migrations, loads the small seed, and starts the API (`:8080`) and the worker. Then it runs `go test ./...`. The API stays up afterwards. To stop everything, run `docker compose down -v`.

For the 500k-deal seed, use `SEED=large docker compose run --build --rm tests`.

To run the API and worker on the host instead (needs Go):

```sh
docker compose up -d db && make migrate-up && make seed-small
make api      # one terminal
make worker   # another
```

## Tests

```sh
make test   # same as: docker compose run --build --rm tests
```

The tests are integration tests against a real Postgres. Each test creates its own freshly migrated database and drops it when it finishes, so tests never see the seed data or each other's data.

To run them on the host instead (needs Go and the `db` container):

```sh
TEST_DATABASE_URL='postgres://postgres:postgres@localhost:5434/postgres?sslmode=disable' go test -count=1 ./...
```

Without `TEST_DATABASE_URL`, every database test is **skipped**, not failed.

The bulk move tests in [internal/bulkmove/](internal/bulkmove/) are grouped by concern:

| File | Covers |
|---|---|
| `submit_test.go` | filter validation, snapshot, the 50,000 cap |
| `idempotency_test.go` | replays, key reuse, concurrent identical submits |
| `concurrency_test.go` | user edits racing the job, lease fencing, concurrent claims |
| `isolation_test.go` | one job per workspace, cross-tenant isolation, user latency during a job |
| `recovery_test.go` | crashes between, during and at the end of chunks |
| `retry_test.go` | retry with backoff, poison jobs, database outages |
| `consistency_test.go` | nothing partial survives a failed or killed chunk |

## Benchmarks

For benchmark results and how to run them, see [BENCHMARKS.md](BENCHMARKS.md).

## Design

For how chunking, idempotency, concurrency and isolation work, and what breaks at 10×, see [DESIGN.md](DESIGN.md).

## What is and isn't implemented

### Implemented

- Opportunities: create, move one deal (with a recorded stage transition), and list a stage with keyset pagination.
- Bulk move submit:
  - required `Idempotency-Key`
  - snapshot of the matching deals at submit time
  - cap of 50,000 deals
  - the status endpoint
- Worker:
  - leases fenced by a token per claim
  - one transaction per chunk, resuming from a cursor after a crash
  - deals edited after submit are skipped
  - retry with backoff
  - poison jobs are failed after 5 attempts with no progress
  - graceful shutdown
- Isolation:
  - a separate connection pool for the worker
  - paced chunks and bounded lock waits
  - at most one running job per workspace, enforced by a unique index
- Migrations, deterministic seeds, the integration test suite, and the benchmark.

### Not implemented

- **Authentication.** `X-Workspace-Id` is trusted as sent.
- **Cancelling, listing or deleting bulk moves.** Re-running a finished or failed move needs a new idempotency key.
- **Updating or deleting opportunities,** other than moving them.
- **An expected-version check on manual moves.** A retried move can overwrite a newer change.
- **Bulk moves over 50,000 deals.** They are rejected with `422`, not run asynchronously.
- **Retention.** Idempotency keys, snapshot items and transitions are kept forever.
- **Metrics and tracing.** There are structured logs only.
- **`idle_in_transaction_session_timeout` on the worker pool.** A worker that stalls mid-chunk keeps its locks until its connection dies (see [DESIGN.md](DESIGN.md) §5).

## Seed data

- **Small:** workspaces `1`–`5`, 2,000–5,000 deals each.
- **Large:** workspace `1`, 500,000 deals.

Each workspace has 12 stages. Workspace 1 uses stage ids `1`–`12`, workspace 2 uses `13`–`24`, and so on. Stages 1–10 are open, 11 is Closed Won and 12 is Closed Lost. Owners are `1`–`40`. Seeding gives the same data and ids on every run.

## API

Every endpoint except `/healthz` needs an `X-Workspace-Id` header. Errors look like `{"error": {"code": "...", "message": "..."}}`.

```sh
H='X-Workspace-Id: 1'
J='Content-Type: application/json'
```

### Health check

```sh
curl localhost:8080/healthz
```

### Create an opportunity

```sh
curl -X POST localhost:8080/opportunities -H "$H" -H "$J" \
  -d '{"name": "Acme", "value_cents": 250000, "stage_id": 1, "owner_id": 7}'
```

### Move an opportunity

```sh
curl -X POST localhost:8080/opportunities/42/move -H "$H" -H "$J" -d '{"stage_id": 3}'
```

Returns the updated opportunity with `version` incremented. Moving it to the stage it is already in changes nothing. Errors: `404` if the opportunity is not in this workspace, `422` if the stage is not, and `503` with `Retry-After: 1` if another request is changing the same deal.

### List opportunities in a stage

```sh
curl 'localhost:8080/stages/1/opportunities?limit=50' -H "$H"
```

`limit` is 1–200. For the next page, pass the response's `next_after` as `?after=`.

### Submit a bulk move

```sh
curl -X POST localhost:8080/bulk-moves -H "$H" -H "$J" -H 'Idempotency-Key: k1' \
  -d '{"filter": {"stage_id": 1, "owner_id": 7}, "target_stage_id": 2}'
```

Returns `202` with the job. Filter fields: `stage_id`, `owner_id`, `status`, `min_value_cents`, `max_value_cents`, `created_from`, `created_to` (exclusive). All are optional. A bulk move covers at most 50,000 deals. Sending the same `Idempotency-Key` again returns the same job.

### Get a bulk move's progress

```sh
curl localhost:8080/bulk-moves/1 -H "$H"
```

```json
{"id": 1, "status": "running", "target_stage_id": 2, "total": 1200, "moved": 800, "skipped": 0,
 "last_error": null, "created_at": "...", "started_at": "...", "finished_at": null}
```

`status` goes from `pending` to `running`, then to `completed` or `failed`. `skipped` counts deals that were already in the target stage or had changed since the job was submitted. Poll until `status` is `completed` or `failed`. Returns `404` if the job belongs to another workspace.
