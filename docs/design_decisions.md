# Design Decisions

This document records the main tradeoffs behind ForgeQueue.

## PostgreSQL as the Queue

ForgeQueue uses PostgreSQL because it provides durability, transactions, indexes, and row-level locking without adding another infrastructure dependency.

This is a good fit for background jobs where correctness and simplicity matter more than extreme throughput.

Tradeoff: PostgreSQL will not scale like Kafka or Redis Streams for very high-throughput event workloads.

## `FOR UPDATE SKIP LOCKED`

Workers claim jobs using `FOR UPDATE SKIP LOCKED`.

This allows many workers to compete for jobs safely. If one worker locks a row, another worker skips it and claims a different job.

Benefits:

- safe concurrent claiming
- no external lock service
- simple PostgreSQL-native design

Tradeoff: workers coordinate through the database, so indexing and connection limits matter.

## Leases Instead of Distributed Locks

ForgeQueue uses lease fields on the job row:

- `locked_by`
- `locked_until`

A worker owns a job only while the lease is valid. Heartbeats extend the lease while the handler runs.

If a worker crashes, the lease expires and another worker can reclaim the job.

Tradeoff: a short lease gives faster crash recovery but creates more heartbeat writes.

## At-Least-Once Delivery

ForgeQueue is at-least-once, not exactly-once.

A job should not be lost, but it can run more than once in rare cases. For example, a worker may perform the side effect and then crash before marking the job completed.

Because of this, job handlers should be idempotent.

## Duplicate Execution Window

There is one important edge case:

1. Worker A claims a job.
2. Worker A stops renewing the lease.
3. The lease expires.
4. Worker B reclaims the job.
5. Worker A may still be running.

For a short time, both workers may execute the same job.

Avoiding this completely would require stronger mechanisms such as fencing tokens or distributed consensus. ForgeQueue keeps the design simpler and requires idempotent handlers.

## Retry and Dead-Letter Flow

On handler failure:

- if retries remain, the job goes back to `pending`
- if retries are exhausted, the job moves to `dead`

The latest failure is stored in `error_message`.

Future improvements:

- explicit non-retryable error type
- configurable retry backoff
- richer failure history

## Claim Query Index

The initial claim query used a sequential scan and sort with 10,000 pending jobs.

A partial index was added:

```sql
CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority_run_at
ON jobs (priority DESC, run_at ASC)
WHERE status = 'pending';
```

After that, PostgreSQL used an index scan for the worker claim path.

## Separate API and Worker Processes

The API and worker are separate commands.

Benefits:

- API can scale separately from workers
- workers can restart without stopping submissions
- deployment is clearer
- metrics are separated by process type

Tradeoff: local development needs two processes, so `scripts/demo.sh` starts both.
