# ForgeQueue Performance Notes

This document records local load-test results for ForgeQueue using k6.

## Environment

These results were collected on a local development machine, not a controlled production environment.

| Component | Value |
|---|---|
| OS | Fedora Linux 42 Workstation |
| Kernel | Linux 6.19.14-108.fc42.x86_64 |
| Machine | ASUS Zenbook UX3402ZA |
| CPU | Intel Core i5-1240P, 12th Gen, 16 logical CPUs |
| Memory | 15.23 GiB RAM |
| Database | PostgreSQL 16 via Docker Compose |
| API | `go run ./cmd/api` |
| Worker | compiled local worker binary via `scripts/run_load_scenarios.sh` |
| Load tool | k6 via Docker |
| Job kind | `test_job` |
| Simulated work per job | ~250ms |

## Load Test Commands

Fixed-rate load test:

```bash
make load-test LOAD_RATE=50 LOAD_DURATION=30s
```

Submit-N-jobs scenario runner:

```bash
./scripts/run_load_scenarios.sh
```

The scenario runner:

1. clears the `jobs` table,
2. starts the worker with the configured worker count,
3. submits jobs using k6,
4. waits until `pending=0` and `running=0`,
5. records API latency, drain time, throughput, and queue latency.

## Fixed-Rate Test Results

| Rate | Duration | Workers | Jobs Created | HTTP Failures | API p95 Latency | Pending After Test | Running After Test | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 10/s | 30s | 5 | 301 | 0 | 5.82ms | 0 | 0 | Kept up |
| 50/s | 30s | 5 | 1501 | 0 | 3.56ms | 544 | 5 | Worker pool saturated |
| 50/s | 30s | 10 | 1501 | 0 | 3.40ms | 0 | 0 | Kept up |
| 50/s | 30s | 20 | 1500 | 0 | 3.59ms | 0 | 0 | Kept up |

## Submit-N-Jobs Scenario Results

| Scenario | Workers | Jobs | API p95 | HTTP Failures | Drain Time | Worker Throughput | Avg Queue Latency | Completed | Dead | Final Pending | Final Running |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 jobs | 1 | 100 | 8.10ms | 0.00% | 27s | 3.70 jobs/s | 13.998s | 100 | 0 | 0 | 0 |
| 1000 jobs | 1 | 1000 | 3.58ms | 0.00% | 263s | 3.80 jobs/s | 131.352s | 1000 | 0 | 0 | 0 |
| 1000 jobs | 5 | 1000 | 4.27ms | 0.00% | 54s | 18.52 jobs/s | 26.516s | 1000 | 0 | 0 | 0 |
| 1000 jobs | 10 | 1000 | 4.35ms | 0.00% | 28s | 35.71 jobs/s | 13.628s | 1000 | 0 | 0 | 0 |

## Interpretation

The API accepted all submitted jobs successfully in every scenario, with `0.00%` HTTP failures. API p95 latency stayed below `10ms` in the submit-N-jobs scenarios, which shows that job submission was not the bottleneck in these tests.

Worker throughput increased significantly as the worker count increased:

```text
1 worker   -> 3.80 jobs/s
5 workers  -> 18.52 jobs/s
10 workers -> 35.71 jobs/s
```

This is close to linear scaling for this local workload, but not perfectly linear. That is expected because workers still coordinate through PostgreSQL using `FOR UPDATE SKIP LOCKED`, and there is overhead from polling, database queries, scheduling, and connection usage.

Average queue latency decreased as more workers were added:

```text
1 worker, 1000 jobs   -> 131.352s
5 workers, 1000 jobs  -> 26.516s
10 workers, 1000 jobs -> 13.628s
```

This confirms that queue latency is strongly affected by worker capacity. When there are not enough workers, jobs wait longer before being claimed. When more workers are available, the queue drains faster and jobs spend less time waiting.

All submit-N-jobs scenarios ended with:

```text
completed = jobs submitted
dead = 0
pending = 0
running = 0
```

So no jobs were lost, stuck, or incorrectly moved to the dead-letter state during these load tests.

## Takeaways

- ForgeQueue handled 1000 jobs with 1, 5, and 10 workers without errors or stuck jobs.
- API latency remained low even while workers were processing a backlog.
- Worker count directly affects throughput and queue latency.
- `forgequeue_queue_depth` is the clearest signal for whether workers are keeping up.
- `forgequeue_queue_latency_seconds` shows the user-visible impact of worker saturation.
- The system currently scales well for this local test workload, but higher worker counts may eventually hit PostgreSQL connection pool or row-locking limits.