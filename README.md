# ForgeQueue

ForgeQueue is a PostgreSQL-backed durable job queue built in Go.

The goal of this project is to learn backend infrastructure concepts deeply:

- Go HTTP services
- PostgreSQL transactions and row-level locking
- Concurrent worker pools
- Retry and dead-letter queue design
- Lease-based crash recovery
- Observability with Prometheus
- Load testing and hardening

## Current Status

Scaffolding phase.

## Planned Architecture

API -> PostgreSQL -> Worker Pool

## Tech Stack

- Go
- PostgreSQL
- Docker Compose
- Prometheus
- sqlc / pgx
- golang-migrate