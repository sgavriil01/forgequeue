package db

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClaimNextJobMarksJobRunning(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create db pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	q := New(pool)

	job, err := q.CreateJob(ctx, CreateJobParams{
		Kind:       "test_job",
		Payload:    []byte(`{"message":"hello"}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := q.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-test-1",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	if claimed.ID != job.ID {
		t.Fatalf("expected claimed job %v, got %v", job.ID, claimed.ID)
	}

	if claimed.Status != JobStatusRunning {
		t.Fatalf("expected status running, got %s", claimed.Status)
	}

	if !claimed.LockedBy.Valid || claimed.LockedBy.String != "worker-test-1" {
		t.Fatalf("expected locked_by worker-test-1, got %+v", claimed.LockedBy)
	}

	if !claimed.LockedUntil.Valid {
		t.Fatalf("expected locked_until to be set")
	}
}

func TestMarkJobCompletedClearsLock(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create db pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	q := New(pool)

	_, err = q.CreateJob(ctx, CreateJobParams{
		Kind:       "test_job",
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := q.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-test-2",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	completed, err := q.MarkJobCompleted(ctx, MarkJobCompletedParams{
		ID:       claimed.ID,
		WorkerID: "worker-test-2",
	})
	if err != nil {
		t.Fatalf("mark completed: %v", err)
	}

	if completed.Status != JobStatusCompleted {
		t.Fatalf("expected status completed, got %s", completed.Status)
	}

	if completed.LockedBy.Valid {
		t.Fatalf("expected locked_by to be cleared")
	}

	if completed.LockedUntil.Valid {
		t.Fatalf("expected locked_until to be cleared")
	}
}

func TestMarkJobFailedClearsLockAndStoresError(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create db pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	q := New(pool)

	_, err = q.CreateJob(ctx, CreateJobParams{
		Kind:       "test_job",
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := q.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-test-3",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	failed, err := q.MarkJobFailed(ctx, MarkJobFailedParams{
		ID:           claimed.ID,
		WorkerID:     "worker-test-3",
		ErrorMessage: "handler failed",
	})
	if err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	if failed.Status != JobStatusFailed {
		t.Fatalf("expected status failed, got %s", failed.Status)
	}

	if !failed.ErrorMessage.Valid || failed.ErrorMessage.String != "handler failed" {
		t.Fatalf("expected error_message handler failed, got %+v", failed.ErrorMessage)
	}

	if failed.LockedBy.Valid {
		t.Fatalf("expected locked_by to be cleared")
	}

	if failed.LockedUntil.Valid {
		t.Fatalf("expected locked_until to be cleared")
	}
}

func truncateJobs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	_, err := pool.Exec(ctx, "TRUNCATE TABLE jobs")
	if err != nil {
		t.Fatalf("truncate jobs: %v", err)
	}
}

func TestScheduleJobRetrySetsRunAtAndErrorMessage(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	queries := New(pool)

	_, err = queries.CreateJob(ctx, CreateJobParams{
		Kind:       "retry_run_at_test",
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-1",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	const delaySeconds int32 = 30

	before := time.Now().UTC()

	retried, err := queries.ScheduleJobRetry(ctx, ScheduleJobRetryParams{
		ID:           claimed.ID,
		WorkerID:     "worker-1",
		ErrorMessage: "temporary failure",
		DelaySeconds: delaySeconds,
	})
	if err != nil {
		t.Fatalf("schedule retry: %v", err)
	}

	after := time.Now().UTC()

	if retried.Status != JobStatusPending {
		t.Fatalf("expected pending, got %s", retried.Status)
	}

	if retried.RetryCount != 1 {
		t.Fatalf("expected retry_count 1, got %d", retried.RetryCount)
	}

	if !retried.ErrorMessage.Valid {
		t.Fatalf("expected error_message to be valid")
	}

	if retried.ErrorMessage.String != "temporary failure" {
		t.Fatalf("expected error_message temporary failure, got %s", retried.ErrorMessage.String)
	}

	earliest := before.Add(time.Duration(delaySeconds) * time.Second)
	latest := after.Add(time.Duration(delaySeconds) * time.Second).Add(1 * time.Second)

	if retried.RunAt.Time.Before(earliest) {
		t.Fatalf("expected run_at after %v, got %v", earliest, retried.RunAt.Time)
	}

	if retried.RunAt.Time.After(latest) {
		t.Fatalf("expected run_at before %v, got %v", latest, retried.RunAt.Time)
	}
}

func TestDeadJobIsNotClaimedAgain(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	queries := New(pool)

	_, err = queries.CreateJob(ctx, CreateJobParams{
		Kind:       "dead_job_claim_test",
		Payload:    []byte(`{}`),
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-1",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	_, err = queries.MarkJobDead(ctx, MarkJobDeadParams{
		ID:           claimed.ID,
		WorkerID:     "worker-1",
		ErrorMessage: "permanent failure",
	})
	if err != nil {
		t.Fatalf("mark job dead: %v", err)
	}

	_, err = queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-2",
		LeaseSeconds: 30,
	})

	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected no claimable jobs, got %v", err)
	}
}

func TestRenewJobLeaseExtendsLockedUntil(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	queries := New(pool)

	_, err = queries.CreateJob(ctx, CreateJobParams{
		Kind:       "lease_renew_test",
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-1",
		LeaseSeconds: 1,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	if !claimed.LockedUntil.Valid {
		t.Fatalf("expected locked_until to be valid")
	}

	oldLockedUntil := claimed.LockedUntil.Time

	time.Sleep(20 * time.Millisecond)

	renewed, err := queries.RenewJobLease(ctx, RenewJobLeaseParams{
		ID:           claimed.ID,
		WorkerID:     "worker-1",
		LeaseSeconds: 60,
	})
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}

	if renewed.Status != JobStatusRunning {
		t.Fatalf("expected running, got %s", renewed.Status)
	}

	if !renewed.LockedUntil.Valid {
		t.Fatalf("expected renewed locked_until to be valid")
	}

	if !renewed.LockedUntil.Time.After(oldLockedUntil) {
		t.Fatalf("expected locked_until to be extended, old=%v new=%v", oldLockedUntil, renewed.LockedUntil.Time)
	}

	if !renewed.LockedBy.Valid {
		t.Fatalf("expected locked_by to be valid")
	}

	if renewed.LockedBy.String != "worker-1" {
		t.Fatalf("expected locked_by worker-1, got %s", renewed.LockedBy.String)
	}
}

func TestReclaimExpiredJobsMovesExpiredRunningJobToPending(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	queries := New(pool)

	_, err = queries.CreateJob(ctx, CreateJobParams{
		Kind:       "expired_reclaim_test",
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-1",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		"UPDATE jobs SET locked_until = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
		claimed.ID,
	)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	reclaimed, err := queries.ReclaimExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim expired jobs: %v", err)
	}

	if len(reclaimed) != 1 {
		t.Fatalf("expected 1 reclaimed job, got %d", len(reclaimed))
	}

	job := reclaimed[0]

	if job.ID != claimed.ID {
		t.Fatalf("expected reclaimed id %v, got %v", claimed.ID, job.ID)
	}

	if job.Status != JobStatusPending {
		t.Fatalf("expected pending, got %s", job.Status)
	}

	if job.RetryCount != 1 {
		t.Fatalf("expected retry_count 1, got %d", job.RetryCount)
	}

	if job.LockedBy.Valid {
		t.Fatalf("expected locked_by to be cleared")
	}

	if job.LockedUntil.Valid {
		t.Fatalf("expected locked_until to be cleared")
	}

	if !job.ErrorMessage.Valid {
		t.Fatalf("expected error_message to be valid")
	}

	expected := "job lease expired; reclaiming for retry"
	if job.ErrorMessage.String != expected {
		t.Fatalf("expected %q, got %q", expected, job.ErrorMessage.String)
	}
}

func TestReclaimExpiredJobsDoesNotReclaimActiveLease(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	queries := New(pool)

	_, err = queries.CreateJob(ctx, CreateJobParams{
		Kind:       "active_lease_test",
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	_, err = queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-1",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	reclaimed, err := queries.ReclaimExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim expired jobs: %v", err)
	}

	if len(reclaimed) != 0 {
		t.Fatalf("expected 0 reclaimed jobs, got %d", len(reclaimed))
	}
}

func TestReclaimExpiredJobsMarksJobDeadWhenRetriesExhausted(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	queries := New(pool)

	_, err = queries.CreateJob(ctx, CreateJobParams{
		Kind:       "expired_dead_test",
		Payload:    []byte(`{}`),
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, err := queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "worker-1",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		"UPDATE jobs SET locked_until = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
		claimed.ID,
	)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	reclaimed, err := queries.ReclaimExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim expired jobs: %v", err)
	}

	if len(reclaimed) != 1 {
		t.Fatalf("expected 1 reclaimed job, got %d", len(reclaimed))
	}

	job := reclaimed[0]

	if job.Status != JobStatusDead {
		t.Fatalf("expected dead, got %s", job.Status)
	}

	if job.RetryCount != 1 {
		t.Fatalf("expected retry_count 1, got %d", job.RetryCount)
	}

	if job.LockedBy.Valid {
		t.Fatalf("expected locked_by to be cleared")
	}

	if job.LockedUntil.Valid {
		t.Fatalf("expected locked_until to be cleared")
	}

	if !job.ErrorMessage.Valid {
		t.Fatalf("expected error_message to be valid")
	}

	expected := "job lease expired and max retries exhausted"
	if job.ErrorMessage.String != expected {
		t.Fatalf("expected %q, got %q", expected, job.ErrorMessage.String)
	}
}

func TestStaleWorkerCannotCompleteAfterLeaseReclaimed(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	truncateJobs(t, ctx, pool)

	queries := New(pool)

	_, err = queries.CreateJob(ctx, CreateJobParams{
		Kind:       "stale_worker_test",
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimedByOldWorker, err := queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "old-worker",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job as old worker: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		"UPDATE jobs SET locked_until = clock_timestamp() - INTERVAL '1 second' WHERE id = $1",
		claimedByOldWorker.ID,
	)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	reclaimed, err := queries.ReclaimExpiredJobs(ctx, 10)
	if err != nil {
		t.Fatalf("reclaim expired jobs: %v", err)
	}

	if len(reclaimed) != 1 {
		t.Fatalf("expected 1 reclaimed job, got %d", len(reclaimed))
	}

	_, err = queries.MarkJobCompleted(ctx, MarkJobCompletedParams{
		ID:       claimedByOldWorker.ID,
		WorkerID: "old-worker",
	})

	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected old worker completion to affect no rows, got %v", err)
	}

	claimedByNewWorker, err := queries.ClaimNextJob(ctx, ClaimNextJobParams{
		WorkerID:     "new-worker",
		LeaseSeconds: 30,
	})
	if err != nil {
		t.Fatalf("claim job as new worker: %v", err)
	}

	if claimedByNewWorker.ID != claimedByOldWorker.ID {
		t.Fatalf("expected same job to be reclaimed, got %v", claimedByNewWorker.ID)
	}

	completed, err := queries.MarkJobCompleted(ctx, MarkJobCompletedParams{
		ID:       claimedByNewWorker.ID,
		WorkerID: "new-worker",
	})
	if err != nil {
		t.Fatalf("mark completed as new worker: %v", err)
	}

	if completed.Status != JobStatusCompleted {
		t.Fatalf("expected completed, got %s", completed.Status)
	}
}