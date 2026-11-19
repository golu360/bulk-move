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

## Benchmarks

For benchmark results and how to run them, see [BENCHMARKS.md](BENCHMARKS.md).

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

### List a stage

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
