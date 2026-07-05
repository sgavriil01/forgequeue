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
| Worker | compiled local worker binary via `scripts/run_load_scenarios.sh` / `scripts/run_backlog_scenario.sh` |
| Load tool | k6 via Docker |
| Job kind | `test_job` |
| Simulated work per job | ~250ms |

## Load Test Commands

Fixed-rate submission test:

```bash
make load-test LOAD_RATE=50 LOAD_DURATION=30s
```

Submit a fixed number of jobs as fast as possible:

```bash
make load-submit LOAD_JOBS=1000 LOAD_VUS=20
```

Run worker-count scenarios:

```bash
./scripts/run_load_scenarios.sh
```

Run backlog scenario:

```bash
./scripts/run_backlog_scenario.sh
```

## Worker-Count Load Test Scenarios

The worker-count scenarios were run using:

```bash
./scripts/run_load_scenarios.sh
```

Each scenario:

1. clears the `jobs` table,
2. starts the worker with the configured worker count,
3. submits jobs using k6,
4. waits until `pending=0` and `running=0`,
5. records API latency, drain time, throughput, and queue latency.

### Results

| Scenario | Workers | Jobs | API p95 | HTTP Failures | Drain Time | Worker Throughput | Avg Queue Latency | Completed | Dead | Final Pending | Final Running |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 jobs | 1 | 100 | 8.10ms | 0.00% | 27s | 3.70 jobs/s | 13.998s | 100 | 0 | 0 | 0 |
| 1000 jobs | 1 | 1000 | 3.58ms | 0.00% | 263s | 3.80 jobs/s | 131.352s | 1000 | 0 | 0 | 0 |
| 1000 jobs | 5 | 1000 | 4.27ms | 0.00% | 54s | 18.52 jobs/s | 26.516s | 1000 | 0 | 0 | 0 |
| 1000 jobs | 10 | 1000 | 4.35ms | 0.00% | 28s | 35.71 jobs/s | 13.628s | 1000 | 0 | 0 | 0 |

### Interpretation

The API accepted all submitted jobs successfully in every scenario, with `0.00%` HTTP failures. API p95 latency stayed below `10ms`, which shows that job submission was not the bottleneck in these tests.

Worker throughput increased significantly as the worker count increased:

```text
1 worker   -> 3.80 jobs/s
5 workers  -> 18.52 jobs/s
10 workers -> 35.71 jobs/s
```

This is close to linear scaling for this local workload, but not perfectly linear. That is expected because workers coordinate through PostgreSQL using `FOR UPDATE SKIP LOCKED`, and there is overhead from polling, database queries, scheduling, and connection usage.

Average queue latency decreased as more workers were added:

```text
1 worker, 1000 jobs   -> 131.352s
5 workers, 1000 jobs  -> 26.516s
10 workers, 1000 jobs -> 13.628s
```

This confirms that queue latency is strongly affected by worker capacity. When there are not enough workers, jobs wait longer before being claimed. When more workers are available, the queue drains faster and jobs spend less time waiting.

All worker-count scenarios ended with:

```text
completed = jobs submitted
dead = 0
pending = 0
running = 0
```

So no jobs were lost, stuck, or incorrectly moved to the dead-letter state during these load tests.

## Backlog Scenario: 10,000 Jobs

This scenario submits 10,000 jobs faster than the worker pool can process them. It tests whether ForgeQueue can build up a large queue, continue processing steadily, and eventually drain the backlog without losing jobs.

### Configuration

| Workers | Jobs Submitted | VUs | Simulated Work |
|---:|---:|---:|---:|
| 5 | 10,000 | 50 | ~250ms/job |

### Results

| API p95 | HTTP Failures | Total Drain Time | Worker Throughput | Avg Queue Latency | Max Pending | Max Running | Final Completed | Final Dead | Final Pending | Final Running |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10.52ms | 0.00% | 533s | 18.76 jobs/s | 266.995s | 9,976 | 5 | 10,000 | 0 | 0 | 0 |

### Queue-Depth Samples

The queue-depth samples showed the expected behavior:

```text
start: pending=9976 running=5 completed=19 dead=0
end:   pending=0    running=0 completed=10000 dead=0
```

### Interpretation

The API accepted 10,000 jobs with `0.00%` HTTP failures and low p95 latency. Because the jobs were submitted much faster than the workers could process them, queue depth grew close to 10,000 pending jobs.

The worker pool then processed the backlog steadily. With 5 workers and ~250ms of simulated work per job, the theoretical maximum is roughly:

```text
5 workers × 4 jobs/sec = 20 jobs/sec
```

The measured throughput was:

```text
18.76 jobs/sec
```

This is close to the expected limit.

The average queue latency was `266.995s`, which is also expected for a large FIFO-style backlog. Since the total drain time was `533s`, the average job waited roughly half the total drain time before being claimed.

The backlog fully drained:

```text
completed = 10,000
dead = 0
pending = 0
running = 0
```

No jobs were lost, stuck, or incorrectly moved to the dead-letter state. This confirms that ForgeQueue can handle a large backlog and eventually drain it with a fixed-size worker pool.

## Takeaways

- ForgeQueue handled 1000 jobs with 1, 5, and 10 workers without errors or stuck jobs.
- ForgeQueue handled a 10,000-job backlog with 5 workers and drained it successfully.
- API latency remained low even while workers were processing a large backlog.
- Worker count directly affects throughput and queue latency.
- `forgequeue_queue_depth` is the clearest signal for whether workers are keeping up.
- `forgequeue_queue_latency_seconds` shows the user-visible impact of worker saturation.
- The system currently scales well for this local test workload, but higher worker counts may eventually hit PostgreSQL connection pool or row-locking limits.