package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCreateAndGetJob(t *testing.T) {
	ctx := context.Background()

	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://forgequeue:forgequeue@localhost:5433/forgequeue?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to create db pool: %v", err)
	}
	defer pool.Close()

	q := New(pool)

	job, err := q.CreateJob(ctx, CreateJobParams{
		Kind:    "test_job",
		Payload: []byte(`{"message":"hello"}`),
		RunAt: pgtype.Timestamptz{
			Time:  time.Now(),
			Valid: true,
		},
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	foundJob, err := q.GetJobByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job by id: %v", err)
	}

	if foundJob.ID != job.ID {
		t.Fatalf("expected job id %v, got %v", job.ID, foundJob.ID)
	}

	if foundJob.Kind != "test_job" {
		t.Fatalf("expected kind test_job, got %s", foundJob.Kind)
	}

	if foundJob.Status != JobStatusPending {
		t.Fatalf("expected status pending, got %s", foundJob.Status)
	}
}