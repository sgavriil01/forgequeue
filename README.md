# ForgeQueue

A durable PostgreSQL-backed job queue built in Go.

ForgeQueue exposes an HTTP API for creating jobs, stores them in PostgreSQL, and runs them through a worker pool with retries, leases, crash recovery, and Prometheus metrics.

## Features

- Durable job storage in PostgreSQL
- HTTP API for creating, listing, reading, and cancelling jobs
- Concurrent workers using `FOR UPDATE SKIP LOCKED`
- Retries and dead-letter jobs
- Lease/heartbeat system for crash recovery
- Prometheus metrics
- k6 load-test scripts
- Demo script for a full local run

## Architecture

```text
Client -> HTTP API -> PostgreSQL <- Worker Pool
```

The API writes jobs to PostgreSQL. Workers claim pending jobs, execute registered handlers, and update job status.

More detail:

- `docs/architecture.md`
- `docs/design_decisions.md`
- `docs/performance.md`

## Quick Start

Start PostgreSQL:

```bash
docker compose up -d
```

Run migrations:

```bash
make migrate
```

Start the API:

```bash
FORGEQUEUE_DATABASE_URL="postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable" \
FORGEQUEUE_HTTP_ADDR=":8080" \
make run-api
```

Start the worker in another terminal:

```bash
FORGEQUEUE_DATABASE_URL="postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable" \
FORGEQUEUE_WORKER_COUNT=5 \
FORGEQUEUE_WORKER_METRICS_ADDR=":9090" \
make run-worker
```

Create a job:

```bash
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"kind":"test_job","payload":{"message":"hello"},"max_retries":3}'
```

Check metrics:

```bash
curl -s localhost:8080/metrics | grep forgequeue_queue_depth
curl -s localhost:9090/metrics | grep forgequeue_active_workers
```

## Demo

Run the full local demo:

```bash
chmod +x scripts/demo.sh
./scripts/demo.sh
```

The demo starts the system, submits jobs, waits for completion, and prints queue depth.

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Create a job |
| `GET` | `/jobs/{id}` | Get a job |
| `GET` | `/jobs` | List jobs |
| `POST` | `/jobs/{id}/cancel` | Cancel a pending job |
| `GET` | `/healthz` | Liveness check |
| `GET` | `/readyz` | Readiness check |
| `GET` | `/metrics` | Prometheus metrics |

Create job request:

```json
{
  "kind": "test_job",
  "payload": {
    "message": "hello"
  },
  "run_at": "2026-01-01T12:00:00Z",
  "max_retries": 3
}
```

Validation rules:

| Field | Rule |
|---|---|
| `kind` | required, non-empty, max 100 chars |
| `payload` | optional JSON object, max 64KB |
| `run_at` | optional RFC3339 timestamp |
| `max_retries` | optional integer, 0-10, defaults to 3 |

## Job Lifecycle

```text
pending -> running -> completed
pending -> cancelled
running -> pending
running -> dead
```

A failed job is retried if retries remain. Otherwise, it moves to `dead`.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FORGEQUEUE_DATABASE_URL` | required | PostgreSQL connection string |
| `FORGEQUEUE_HTTP_ADDR` | `:8080` | API listen address |
| `FORGEQUEUE_WORKER_COUNT` | `5` | Number of workers |
| `FORGEQUEUE_WORKER_METRICS_ADDR` | `:9090` | Worker metrics address |

## Development

Run tests:

```bash
go test -count=1 ./...
```

Run the race detector:

```bash
go test -race -count=1 ./...
```

Run lint:

```bash
golangci-lint run ./...
```

Run load tests:

```bash
./scripts/run_load_scenarios.sh
./scripts/run_backlog_scenario.sh
./scripts/run_crash_reclaim_scenario.sh
```

## Performance Summary

Local results:

| Scenario | Result |
|---|---:|
| 1 worker, 1000 jobs | 3.80 jobs/s |
| 5 workers, 1000 jobs | 18.52 jobs/s |
| 10 workers, 1000 jobs | 35.71 jobs/s |
| 5 workers, 10,000 jobs | 18.76 jobs/s |
| crash/reclaim test | 5 completed, 0 dead |

The worker claim query was improved with a partial index:

```sql
CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority_run_at
ON jobs (priority DESC, run_at ASC)
WHERE status = 'pending';
```

This changed the local claim path from a sequential scan and sort to an index scan.

## Limitations

- At-least-once delivery, not exactly-once
- Handlers must be idempotent
- No multi-tenancy yet
- No dashboard
- No job retention/archival policy yet
- Not intended for Kafka-level event streaming throughput

## Future Work

- Grafana dashboard
- POST /jobs/{id}/retry endpoint
- LISTEN/NOTIFY to wake idle workers
- tenant_id / multi-tenancy
- pprof profiling
- testcontainers-go integration tests

## License

See `LICENSE`.