-- name: CreateJob :one
INSERT INTO jobs (
    kind,
    payload,
    run_at,
    max_retries
)
VALUES (
    sqlc.arg(kind),
    sqlc.arg(payload),
    COALESCE(sqlc.narg(run_at), NOW()),
    sqlc.arg(max_retries)
)
RETURNING *;

-- name: GetJobByID :one
SELECT *
FROM jobs
WHERE id = sqlc.arg(id);

-- name: ListJobs :many
SELECT *
FROM jobs
ORDER BY created_at DESC
LIMIT sqlc.arg(job_limit);

-- name: ListJobsByStatus :many
SELECT *
FROM jobs
WHERE status = sqlc.arg(status)
ORDER BY created_at DESC
LIMIT sqlc.arg(job_limit);

-- name: CancelPendingJob :one
UPDATE jobs
SET status = 'cancelled',
    updated_at = NOW()
WHERE id = sqlc.arg(id)
  AND status = 'pending'
RETURNING *;

-- name: ClaimNextJob :one
WITH claimed AS (
    SELECT id
    FROM jobs
    WHERE status = 'pending'
      AND run_at <= clock_timestamp()
    ORDER BY priority DESC, run_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET status = 'running',
    locked_by = sqlc.arg(worker_id)::text,
    locked_until = clock_timestamp() + (sqlc.arg(lease_seconds)::int * INTERVAL '1 second'),
    attempted_at = clock_timestamp(),
    updated_at = clock_timestamp()
FROM claimed
WHERE jobs.id = claimed.id
RETURNING jobs.*;

-- name: MarkJobCompleted :one
UPDATE jobs
SET status = 'completed',
    completed_at = clock_timestamp(),
    updated_at = clock_timestamp(),
    locked_by = NULL,
    locked_until = NULL
WHERE id = sqlc.arg(id)
  AND locked_by = sqlc.arg(worker_id)::text
  AND status = 'running'
RETURNING *;

-- name: MarkJobFailed :one
UPDATE jobs
SET status = 'failed',
    error_message = sqlc.arg(error_message)::text,
    updated_at = clock_timestamp(),
    locked_by = NULL,
    locked_until = NULL
WHERE id = sqlc.arg(id)
  AND locked_by = sqlc.arg(worker_id)::text
  AND status = 'running'
RETURNING *;

-- name: Ping :one
SELECT 1;