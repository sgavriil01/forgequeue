package db

import (
	"context"
	"os"
	"testing"

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