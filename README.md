# ForgeQueue

A durable PostgreSQL-backed job queue built in Go.

ForgeQueue lets an API accept jobs, stores them safely in PostgreSQL, and lets worker processes claim and execute them. It includes retries, dead-letter jobs, lease-based crash recovery, Prometheus metrics, a Grafana dashboard, and k6 load tests.

## Why this project exists

This project demonstrates backend infrastructure concepts in a small, readable system:

- durable job storage
- safe concurrent workers
- PostgreSQL row locking with `FOR UPDATE SKIP LOCKED`
- retries and dead-letter handling
- worker crash recovery with leases and heartbeats
- observability with Prometheus and Grafana
- load testing and performance documentation

## Architecture

```text
Client -> HTTP API -> PostgreSQL <- Worker Pool
```

The API creates and reads jobs. PostgreSQL stores the queue state. Workers claim pending jobs, execute handlers, and update the job status.

Read more:

- [Architecture](docs/architecture.md)
- [Design decisions](docs/design_decisions.md)
- [Performance notes](docs/performance.md)

## Features

- HTTP API for creating, listing, reading, and cancelling jobs
- PostgreSQL-backed durable queue
- Worker pool with configurable worker count
- Concurrent claiming with `FOR UPDATE SKIP LOCKED`
- Retry and dead-letter flow
- Lease and heartbeat system
- Expired lease reclaiming after worker crashes
- Prometheus metrics
- Grafana dashboard
- k6 load-test scripts
- Local demo script

## Quick Start

Start PostgreSQL:

```bash
docker compose up -d postgres
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

Submit a job:

```bash
curl -X POST localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"kind":"test_job","payload":{"message":"hello"},"max_retries":3}'
```

Check queue depth:

```bash
curl -s localhost:8080/metrics | grep forgequeue_queue_depth
```

## Demo

Run the full local demo:

```bash
chmod +x scripts/demo.sh
./scripts/demo.sh
```

The demo starts the system, submits jobs, waits for them to finish, and prints the final queue state.

## Monitoring

Start Prometheus and Grafana:

```bash
docker compose up -d prometheus grafana
```

Open:

- Prometheus: <http://localhost:9091>
- Grafana: <http://localhost:3000>

Grafana login:

```text
admin / admin
```

The ForgeQueue dashboard is provisioned from:

- [Grafana dashboard JSON](grafana/dashboards/forgequeue.json)

Prometheus config:

- [Prometheus config](prometheus/prometheus.yml)

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

Example job:

```json
{
  "kind": "test_job",
  "payload": {
    "message": "hello"
  },
  "max_retries": 3
}
```

## Job Lifecycle

```text
pending -> running -> completed
pending -> cancelled
running -> pending
running -> dead
```

A failed job is retried while retries remain. After retries are exhausted, it moves to `dead`.

## Performance

Local load-test results:

| Scenario | Result |
|---|---:|
| 1 worker, 1000 jobs | 3.80 jobs/s |
| 5 workers, 1000 jobs | 18.52 jobs/s |
| 10 workers, 1000 jobs | 35.71 jobs/s |
| 5 workers, 10,000 jobs | 18.76 jobs/s |
| Crash/reclaim test | 5 completed, 0 dead |

Full results are in [docs/performance.md](docs/performance.md).

The worker claim query was also checked with `EXPLAIN ANALYZE`. Adding a partial index changed the claim path from a sequential scan and sort to an index scan.

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

## Project Docs

- [Architecture](docs/architecture.md)
- [Design decisions](docs/design_decisions.md)
- [Performance notes](docs/performance.md)
- [Grafana dashboard](grafana/dashboards/forgequeue.json)
- [Prometheus config](prometheus/prometheus.yml)

## Limitations

- At-least-once delivery, not exactly-once
- Job handlers should be idempotent
- No multi-tenancy yet
- No job retention or archival policy yet
- No web dashboard for job management
- Not intended for Kafka-level event streaming throughput

## Future Work

- `POST /jobs/{id}/retry`
- LISTEN/NOTIFY to reduce polling latency
- Configurable retry backoff
- Job retention / archival
- pprof profiling
- OpenTelemetry tracing
- testcontainers-go integration tests
- goleak checks

## License

This project is licensed under the terms in [LICENSE](LICENSE).