# ForgeQueue Performance Notes

This document records local load-test results for ForgeQueue using k6.

## Environment

These results were collected on a local development machine, not a controlled production environment.

| Component | Value |
|---|---|
| OS | Fedora Linux 42 Workstation |
| Kernel | Linux 6.19.14-108.fc42.x86_64 |
| Machine | ASUS Zenbook UX3402ZA |
| CPU | Intel Core i5-1240P, 12th Gen |
| CPU cores | 12 total cores: 4 performance cores + 8 efficiency cores |
| Logical CPUs / threads | 16 |
| Memory | 15.23 GiB RAM |
| Database | PostgreSQL 16 via Docker Compose |
| API | `go run ./cmd/api` |
| Worker | compiled local worker binary via test scripts |
| Load tool | k6 via Docker |
| Job kind | `test_job` |
| Simulated work per normal job | ~250ms |
| Simulated work per slow job | ~60s |

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

Run crash/reclaim scenario:

```bash
./scripts/run_crash_reclaim_scenario.sh
```

## Worker-Count Load Test Scenarios

These scenarios test how throughput changes as worker count increases.

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

The API accepted all submitted jobs successfully in every scenario, with `0.00%` HTTP failures. API p95 latency stayed below `10ms`, so job submission was not the bottleneck.

Worker throughput improved as worker count increased:

```text
1 worker   -> 3.80 jobs/s
5 workers  -> 18.52 jobs/s
10 workers -> 35.71 jobs/s
```

This is close to linear scaling for this local workload, but not perfect. The gap is expected because workers still coordinate through PostgreSQL using `FOR UPDATE SKIP LOCKED`, and there is overhead from polling, database queries, scheduling, and connection usage.

Average queue latency decreased as more workers were added:

```text
1 worker, 1000 jobs   -> 131.352s
5 workers, 1000 jobs  -> 26.516s
10 workers, 1000 jobs -> 13.628s
```

All worker-count scenarios ended with:

```text
completed = jobs submitted
dead = 0
pending = 0
running = 0
```

No jobs were lost, stuck, or incorrectly moved to the dead-letter state.

## Backlog Scenario: 10,000 Jobs

This scenario submits 10,000 jobs faster than the worker pool can process them. It tests whether ForgeQueue can build up a large queue, process steadily, and eventually drain the backlog without losing jobs.

### Configuration

| Workers | Jobs Submitted | VUs | Simulated Work |
|---:|---:|---:|---:|
| 5 | 10,000 | 50 | ~250ms/job |

### Results

| API p95 | HTTP Failures | Total Drain Time | Worker Throughput | Avg Queue Latency | Max Pending | Max Running | Final Completed | Final Dead | Final Pending | Final Running |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10.52ms | 0.00% | 533s | 18.76 jobs/s | 266.995s | 9,976 | 5 | 10,000 | 0 | 0 | 0 |

### Queue-Depth Samples

```text
start: pending=9976 running=5 completed=19 dead=0
end:   pending=0    running=0 completed=10000 dead=0
```

### Interpretation

The API accepted 10,000 jobs with `0.00%` HTTP failures and low p95 latency. Because the jobs were submitted much faster than the workers could process them, queue depth grew close to 10,000 pending jobs.

The worker pool then drained the backlog steadily. With 5 workers and ~250ms of simulated work per job, the rough theoretical maximum is:

```text
5 workers × 4 jobs/sec = 20 jobs/sec
```

Measured throughput was:

```text
18.76 jobs/sec
```

The average queue latency was `266.995s`, which is expected for a large backlog. Since total drain time was `533s`, the average job waited roughly half the drain time before being claimed.

Final state:

```text
completed = 10,000
dead = 0
pending = 0
running = 0
```

No jobs were lost, stuck, or incorrectly moved to the dead-letter state.

## Crash/Reclaim Scenario

This scenario verifies that ForgeQueue can recover from a worker process crash while a job is running.

Because the normal worker pool uses goroutines inside one process, this test starts 5 separate worker processes with `FORGEQUEUE_WORKER_COUNT=1`. One process is then killed with `kill -9` while processing a slow job.

### Configuration

| Worker Processes | Workers per Process | Jobs | Job Kind | Simulated Work |
|---:|---:|---:|---|---:|
| 5 | 1 | 5 | `slow_test_job` | ~60s/job |

### Results

| Killed Workers | Time After Kill Until Completion | Completed | Dead | Pending | Running | Jobs With Retry/Reclaim Count |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 123s | 5 | 0 | 0 | 0 | 1 |

### Interpretation

The killed worker left one job in `running` state with an active lease. The job stayed in `running` until the lease expired. After lease expiry, the reclaimer moved the job back to the queue, and a surviving worker picked it up and completed it.

Final state:

```text
completed = 5
dead = 0
pending = 0
running = 0
jobs with retry/reclaim count = 1
```

This confirms that ForgeQueue can recover from a worker process crash without losing jobs or leaving jobs stuck forever.

## Database Claim Query

The worker claim query was tested with `EXPLAIN ANALYZE` using 10,000 pending jobs. This query is important because workers run it repeatedly to claim the next available job.

Before adding the dedicated claim index, PostgreSQL scanned and sorted the pending jobs:

```text
Seq Scan on jobs
Sort
Execution Time: 2.509 ms
```

A partial index was added for the worker claim path:

```sql
CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority_run_at
ON jobs (priority DESC, run_at ASC)
WHERE status = 'pending';
```

After adding the index, PostgreSQL used the index directly:

```text
Index Scan using idx_jobs_pending_priority_run_at
Execution Time: 0.168 ms
```

This improves the core worker operation because claiming the next pending job no longer requires scanning and sorting the full pending queue.

## Race Testing

The full Go test suite was run with the race detector:

```bash
go test -race -count=1 ./...
```

The test suite passed without detecting data races.

## Takeaways

- ForgeQueue handled 1,000 jobs with 1, 5, and 10 workers without errors or stuck jobs.
- ForgeQueue handled a 10,000-job backlog with 5 workers and drained it successfully.
- ForgeQueue recovered from a killed worker process and reclaimed the in-progress job.
- API latency remained low even while workers were processing a large backlog.
- Worker count directly affects throughput and queue latency.
- `forgequeue_queue_depth` is the clearest signal for whether workers are keeping up.
- `forgequeue_queue_latency_seconds` shows the user-visible impact of worker saturation.
- The claim-query index changed the worker claim path from a sequential scan and sort to an index scan.
- Higher worker counts may eventually hit PostgreSQL connection pool or row-locking limits.