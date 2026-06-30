package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
)

func TestWorkerPoolProcesses100JobsExactlyOnce(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool := openTestDB(t, ctx, dbURL)
	defer pool.Close()

	truncateJobsForWorkerTest(t, ctx, pool)

	queries := db.New(pool)

	const totalJobs = 100
	kind := "test_job_exactly_once"

	for i := 0; i < totalJobs; i++ {
		_, err := queries.CreateJob(ctx, db.CreateJobParams{
			Kind:       kind,
			Payload:    []byte(`{}`),
			MaxRetries: 3,
		})
		if err != nil {
			t.Fatalf("create job %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	processed := make(map[[16]byte]int)

	registry, err := NewRegistry(
		NewHandlerFunc(kind, func(ctx context.Context, job db.Job) error {
			key := job.ID.Bytes

			mu.Lock()
			processed[key]++
			count := processed[key]
			mu.Unlock()

			if count > 1 {
				return errors.New("duplicate processing detected")
			}

			return nil
		}),
	)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	workerPool := NewExecutorPool(
		queries,
		registry,
		PoolConfig{
			NumWorkers:   5,
			PollInterval: 5 * time.Millisecond,
			IdleJitter:   1 * time.Millisecond,
			LeaseSeconds: 30,
		},
		discardLogger(),
	)

	workerPool.Start(ctx)
	defer workerPool.Stop()

	waitUntilIntegration(t, func() bool {
		return countJobsByKindAndStatus(t, ctx, pool, kind, db.JobStatusCompleted) == totalJobs
	}, 10*time.Second)

	workerPool.Stop()

	completed := countJobsByKindAndStatus(t, ctx, pool, kind, db.JobStatusCompleted)
	if completed != totalJobs {
		t.Fatalf("expected %d completed jobs, got %d", totalJobs, completed)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(processed) != totalJobs {
		t.Fatalf("expected %d unique processed jobs, got %d", totalJobs, len(processed))
	}

	for jobID, count := range processed {
		if count != 1 {
			t.Fatalf("job %+v processed %d times", jobID, count)
		}
	}
}

func TestSingleJobIsClaimedByExactlyOneWorker(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool := openTestDB(t, ctx, dbURL)
	defer pool.Close()

	truncateJobsForWorkerTest(t, ctx, pool)

	queries := db.New(pool)

	kind := "test_job_single_claim"

	_, err := queries.CreateJob(ctx, db.CreateJobParams{
		Kind:       kind,
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	var mu sync.Mutex
	handlerCalls := 0

	registry, err := NewRegistry(
		NewHandlerFunc(kind, func(ctx context.Context, job db.Job) error {
			mu.Lock()
			handlerCalls++
			mu.Unlock()

			time.Sleep(25 * time.Millisecond)

			return nil
		}),
	)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	workerPool := NewExecutorPool(
		queries,
		registry,
		PoolConfig{
			NumWorkers:   5,
			PollInterval: 5 * time.Millisecond,
			IdleJitter:   1 * time.Millisecond,
			LeaseSeconds: 30,
		},
		discardLogger(),
	)

	workerPool.Start(ctx)
	defer workerPool.Stop()

	waitUntilIntegration(t, func() bool {
		return countJobsByKindAndStatus(t, ctx, pool, kind, db.JobStatusCompleted) == 1
	}, 5*time.Second)

	workerPool.Stop()

	mu.Lock()
	defer mu.Unlock()

	if handlerCalls != 1 {
		t.Fatalf("expected handler to be called once, got %d", handlerCalls)
	}
}

func TestWorkerPoolRecoversFromPanicAndContinues(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool := openTestDB(t, ctx, dbURL)
	defer pool.Close()

	truncateJobsForWorkerTest(t, ctx, pool)

	queries := db.New(pool)

	panicKind := "panic_job"
	normalKind := "test_job_after_panic"

	_, err := queries.CreateJob(ctx, db.CreateJobParams{
		Kind:       panicKind,
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create panic job: %v", err)
	}

	_, err = queries.CreateJob(ctx, db.CreateJobParams{
		Kind:       normalKind,
		Payload:    []byte(`{}`),
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatalf("create normal job: %v", err)
	}

	registry, err := NewRegistry(
		NewHandlerFunc(panicKind, func(ctx context.Context, job db.Job) error {
			panic("boom")
		}),
		NewHandlerFunc(normalKind, func(ctx context.Context, job db.Job) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	workerPool := NewExecutorPool(
		queries,
		registry,
		PoolConfig{
			NumWorkers:   2,
			PollInterval: 5 * time.Millisecond,
			IdleJitter:   1 * time.Millisecond,
			LeaseSeconds: 30,
		},
		discardLogger(),
	)

	workerPool.Start(ctx)
	defer workerPool.Stop()

	waitUntilIntegration(t, func() bool {
		failed := countJobsByKindAndStatus(t, ctx, pool, panicKind, db.JobStatusFailed)
		completed := countJobsByKindAndStatus(t, ctx, pool, normalKind, db.JobStatusCompleted)

		return failed == 1 && completed == 1
	}, 5*time.Second)

	workerPool.Stop()

	failed := countJobsByKindAndStatus(t, ctx, pool, panicKind, db.JobStatusFailed)
	completed := countJobsByKindAndStatus(t, ctx, pool, normalKind, db.JobStatusCompleted)

	if failed != 1 {
		t.Fatalf("expected 1 failed job, got %d", failed)
	}

	if completed != 1 {
		t.Fatalf("expected 1 completed job, got %d", completed)
	}
}

func TestWorkerPoolShutdownLeavesNoRunningJobs(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool := openTestDB(t, ctx, dbURL)
	defer pool.Close()

	truncateJobsForWorkerTest(t, ctx, pool)

	queries := db.New(pool)

	const totalJobs = 10
	kind := "slow_job_shutdown"

	for i := 0; i < totalJobs; i++ {
		_, err := queries.CreateJob(ctx, db.CreateJobParams{
			Kind:       kind,
			Payload:    []byte(`{}`),
			MaxRetries: 3,
		})
		if err != nil {
			t.Fatalf("create slow job %d: %v", i, err)
		}
	}

	registry, err := NewRegistry(
		NewHandlerFunc(kind, func(ctx context.Context, job db.Job) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	workerPool := NewExecutorPool(
		queries,
		registry,
		PoolConfig{
			NumWorkers:   5,
			PollInterval: 5 * time.Millisecond,
			IdleJitter:   1 * time.Millisecond,
			LeaseSeconds: 30,
		},
		discardLogger(),
	)

	workerPool.Start(ctx)
	defer workerPool.Stop()

	waitUntilIntegration(t, func() bool {
		return countJobsByKindAndStatus(t, ctx, pool, kind, db.JobStatusRunning) > 0
	}, 5*time.Second)

	workerPool.Stop()

	running := countJobsByKindAndStatus(t, ctx, pool, kind, db.JobStatusRunning)
	if running != 0 {
		t.Fatalf("expected 0 running jobs after shutdown, got %d", running)
	}
}

func openTestDB(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("create db pool: %v", err)
	}

	return pool
}

func truncateJobsForWorkerTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	_, err := pool.Exec(ctx, "TRUNCATE TABLE jobs")
	if err != nil {
		t.Fatalf("truncate jobs: %v", err)
	}
}

func countJobsByKindAndStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string, status db.JobStatus) int {
	t.Helper()

	var count int

	err := pool.QueryRow(
		ctx,
		"SELECT COUNT(*) FROM jobs WHERE kind = $1 AND status = $2",
		kind,
		string(status),
	).Scan(&count)
	if err != nil {
		t.Fatalf("count jobs by kind/status %s/%s: %v", kind, status, err)
	}

	return count
}

func waitUntilIntegration(t *testing.T, condition func() bool, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not met before timeout")
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}