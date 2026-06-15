DROP INDEX IF EXISTS idx_jobs_running_locked_until;
DROP INDEX IF EXISTS idx_jobs_status;
DROP INDEX IF EXISTS idx_jobs_pending;

DROP TABLE IF EXISTS jobs;

DROP TYPE IF EXISTS job_status;