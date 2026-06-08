# ForgeQueue

ForgeQueue is a durable PostgreSQL-backed job queue built in Go.

## Current Status

Phase 0: project scaffold, config loading, structured logging, health endpoints, and graceful shutdown.

## Run API

```bash
export FORGEQUEUE_DATABASE_URL="postgres://forgequeue:forgequeue@localhost:5432/forgequeue?sslmode=disable"
go run ./cmd/api