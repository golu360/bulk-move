# Benchmarks

| | Large seed<br>1 workspace, 500k deals | Small seed<br>5 workspaces, 21k deals |
|---|---|---|
| **Bulk move** wall-clock | 7.1 s (50,000 deals) | 0.95 s (21,013 deals, 5 jobs) |
| **Throughput** | 7,265 items/s | 23,848 items/s |
| **List stage** p95 / p99 during bulk | 2.3 / 2.6 ms | 2.1 / 2.4 ms |
| **Move deal** p95 / p99 during bulk | 5.5 / 9.8 ms | 4.0 / 4.3 ms |
| **Kill and resume** time to completion | 9.6 s | 2.7 s |
| **Kill and resume** correct? | ✅ 6/6 checks | ✅ 6/6 checks, all 5 jobs |

## Run it

```sh
docker compose up -d db
make migrate-up
make bench-large   # or: make bench-small
```

Re-seeds the database first. Stop your own API and worker before running.

<details>
<summary><b>Hardware</b></summary>

- Apple M4 Pro, 12 cores, 24 GB RAM, macOS 15.7.4
- Postgres 16.16 in Docker Desktop (12 CPUs, 7.7 GB)
- Go 1.26.0; API, worker and bench run on the host

</details>

<details>
<summary><b>Test cases</b></summary>

1. **Baseline:** 200 req/s of interactive traffic, no bulk job.
2. **Bulk move:** one bulk move per workspace (its oldest 50,000 deals) with the same traffic running.
3. **Kill and resume:** another bulk move; the worker is killed at 50% and a new one started. Then checked in SQL:
   - job completed, moved + skipped = total
   - every deal is in the target stage
   - one transition per moved deal, none duplicated
   - every deal written exactly once (version check)

Traffic mix: 60% list stage, 30% move one deal, 10% job status.
Worker: 4 workers, chunks of 500, 2 s lease.

</details>

<details>
<summary><b>Latency: baseline vs during bulk move</b></summary>

p95 / p99, in ms.

| Endpoint | Large: baseline | Large: during bulk | Small: baseline | Small: during bulk |
|---|---|---|---|---|
| List stage | 3.3 / 4.2 | 2.3 / 2.6 | 3.3 / 4.4 | 2.1 / 2.4 |
| Move deal | 7.2 / 9.9 | 5.5 / 9.8 | 7.2 / 10.3 | 4.0 / 4.3 |
| Job status | — | 1.5 / 1.9 | — | 1.5 / 1.5 |

No errors in any run.

</details>

<details>
<summary><b>Kill and resume details</b></summary>

| | Large | Small |
|---|---|---|
| Killed at | 25,000 / 50,000 (50%) | 12,500 / 21,013 (59%) |
| Wait before resuming | 2.0 s | 2.0 s |
| Time to completion | 9.6 s | 2.7 s |

</details>
