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
WHERE id = $1;

-- name: ListJobs :many
SELECT *
FROM jobs
WHERE ($1::job_status IS NULL OR status = $1)
ORDER BY created_at DESC
LIMIT $2;