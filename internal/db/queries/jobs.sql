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

-- name: Ping :one
SELECT 1;