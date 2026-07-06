CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority_run_at
ON jobs (priority DESC, run_at ASC)
WHERE status = 'pending';
