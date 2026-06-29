package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
)

type fakeStore struct {
	claimJob db.Job
	claimErr error

	completedCalled bool
	completedParams db.MarkJobCompletedParams
	completedErr    error

	failedCalled bool
	failedParams db.MarkJobFailedParams
	failedErr    error
}

func (f *fakeStore) ClaimNextJob(ctx context.Context, arg db.ClaimNextJobParams) (db.Job, error) {
	if f.claimErr != nil {
		return db.Job{}, f.claimErr
	}

	return f.claimJob, nil
}

func (f *fakeStore) MarkJobCompleted(ctx context.Context, arg db.MarkJobCompletedParams) (db.Job, error) {
	f.completedCalled = true
	f.completedParams = arg

	if f.completedErr != nil {
		return db.Job{}, f.completedErr
	}

	return db.Job{ID: arg.ID, Status: db.JobStatusCompleted}, nil
}

func (f *fakeStore) MarkJobFailed(ctx context.Context, arg db.MarkJobFailedParams) (db.Job, error) {
	f.failedCalled = true
	f.failedParams = arg

	if f.failedErr != nil {
		return db.Job{}, f.failedErr
	}

	return db.Job{ID: arg.ID, Status: db.JobStatusFailed}, nil
}

func testUUID() pgtype.UUID {
	return pgtype.UUID{
		Bytes: [16]byte{1, 2, 3, 4, 5},
		Valid: true,
	}
}

func TestExecuteOnceReturnsFalseWhenNoJobAvailable(t *testing.T) {
	store := &fakeStore{
		claimErr: pgx.ErrNoRows,
	}

	registry, err := NewRegistry()
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	executor := NewExecutor(store, registry, "worker-1", 30, nil)

	processed, err := executor.ExecuteOnce(context.Background())
	if err != nil {
		t.Fatalf("execute once: %v", err)
	}

	if processed {
		t.Fatalf("expected processed=false")
	}

	if store.completedCalled {
		t.Fatalf("did not expect completed to be called")
	}

	if store.failedCalled {
		t.Fatalf("did not expect failed to be called")
	}
}

func TestExecuteOnceMarksJobCompletedWhenHandlerSucceeds(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:     jobID,
			Kind:   "test_job",
			Status: db.JobStatusRunning,
		},
	}

	handler := NewHandlerFunc("test_job", func(ctx context.Context, job db.Job) error {
		return nil
	})

	registry, err := NewRegistry(handler)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	executor := NewExecutor(store, registry, "worker-1", 30, nil)

	processed, err := executor.ExecuteOnce(context.Background())
	if err != nil {
		t.Fatalf("execute once: %v", err)
	}

	if !processed {
		t.Fatalf("expected processed=true")
	}

	if !store.completedCalled {
		t.Fatalf("expected completed to be called")
	}

	if store.failedCalled {
		t.Fatalf("did not expect failed to be called")
	}

	if store.completedParams.ID != jobID {
		t.Fatalf("expected completed id %v, got %v", jobID, store.completedParams.ID)
	}

	if store.completedParams.WorkerID != "worker-1" {
		t.Fatalf("expected worker-1, got %s", store.completedParams.WorkerID)
	}
}

func TestExecuteOnceMarksJobFailedWhenHandlerReturnsError(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:     jobID,
			Kind:   "test_job",
			Status: db.JobStatusRunning,
		},
	}

	handler := NewHandlerFunc("test_job", func(ctx context.Context, job db.Job) error {
		return errors.New("handler failed")
	})

	registry, err := NewRegistry(handler)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	executor := NewExecutor(store, registry, "worker-1", 30, nil)

	processed, err := executor.ExecuteOnce(context.Background())
	if err != nil {
		t.Fatalf("execute once: %v", err)
	}

	if !processed {
		t.Fatalf("expected processed=true")
	}

	if store.completedCalled {
		t.Fatalf("did not expect completed to be called")
	}

	if !store.failedCalled {
		t.Fatalf("expected failed to be called")
	}

	if store.failedParams.ErrorMessage != "handler failed" {
		t.Fatalf("expected handler failed, got %s", store.failedParams.ErrorMessage)
	}
}

func TestExecuteOnceMarksJobFailedWhenHandlerMissing(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:     jobID,
			Kind:   "unknown_job",
			Status: db.JobStatusRunning,
		},
	}

	registry, err := NewRegistry()
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	executor := NewExecutor(store, registry, "worker-1", 30, nil)

	processed, err := executor.ExecuteOnce(context.Background())
	if err != nil {
		t.Fatalf("execute once: %v", err)
	}

	if !processed {
		t.Fatalf("expected processed=true")
	}

	if store.completedCalled {
		t.Fatalf("did not expect completed to be called")
	}

	if !store.failedCalled {
		t.Fatalf("expected failed to be called")
	}
}

func TestExecuteOnceMarksJobFailedWhenHandlerPanics(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:     jobID,
			Kind:   "test_job",
			Status: db.JobStatusRunning,
		},
	}

	handler := NewHandlerFunc("test_job", func(ctx context.Context, job db.Job) error {
		panic("boom")
	})

	registry, err := NewRegistry(handler)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	executor := NewExecutor(store, registry, "worker-1", 30, nil)

	processed, err := executor.ExecuteOnce(context.Background())
	if err != nil {
		t.Fatalf("execute once: %v", err)
	}

	if !processed {
		t.Fatalf("expected processed=true")
	}

	if store.completedCalled {
		t.Fatalf("did not expect completed to be called")
	}

	if !store.failedCalled {
		t.Fatalf("expected failed to be called")
	}
}