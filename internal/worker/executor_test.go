package worker

import (
	"context"
	"errors"
	"testing"
	"time"

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

	retryCalled bool
	retryParams db.ScheduleJobRetryParams
	retryErr    error

	deadCalled bool
	deadParams db.MarkJobDeadParams
	deadErr    error

	renewCalled bool
	renewParams db.RenewJobLeaseParams
	renewErr    error

	renewFunc func(ctx context.Context, arg db.RenewJobLeaseParams) (db.Job, error)
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

func (f *fakeStore) ScheduleJobRetry(ctx context.Context, arg db.ScheduleJobRetryParams) (db.Job, error) {
	f.retryCalled = true
	f.retryParams = arg

	if f.retryErr != nil {
		return db.Job{}, f.retryErr
	}

	return db.Job{
		ID:         arg.ID,
		Status:     db.JobStatusPending,
		RetryCount: f.claimJob.RetryCount + 1,
		MaxRetries: f.claimJob.MaxRetries,
	}, nil
}

func (f *fakeStore) MarkJobDead(ctx context.Context, arg db.MarkJobDeadParams) (db.Job, error) {
	f.deadCalled = true
	f.deadParams = arg

	if f.deadErr != nil {
		return db.Job{}, f.deadErr
	}

	return db.Job{
		ID:         arg.ID,
		Status:     db.JobStatusDead,
		RetryCount: f.claimJob.RetryCount + 1,
		MaxRetries: f.claimJob.MaxRetries,
	}, nil
}

func testUUID() pgtype.UUID {
	return pgtype.UUID{
		Bytes: [16]byte{1, 2, 3, 4, 5},
		Valid: true,
	}
}

func (f *fakeStore) RenewJobLease(ctx context.Context, arg db.RenewJobLeaseParams) (db.Job, error) {
	f.renewCalled = true
	f.renewParams = arg

	if f.renewFunc != nil {
		return f.renewFunc(ctx, arg)
	}

	if f.renewErr != nil {
		return db.Job{}, f.renewErr
	}

	return db.Job{
		ID:          arg.ID,
		Status:      db.JobStatusRunning,
		RetryCount:  f.claimJob.RetryCount,
		MaxRetries:  f.claimJob.MaxRetries,
		LockedUntil: pgtype.Timestamptz{Time: time.Now().Add(time.Duration(arg.LeaseSeconds) * time.Second), Valid: true},
	}, nil
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

	if store.retryCalled {
		t.Fatalf("did not expect retry to be called")
	}

	if store.deadCalled {
		t.Fatalf("did not expect dead to be called")
	}
}

func TestExecuteOnceMarksJobCompletedWhenHandlerSucceeds(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:         jobID,
			Kind:       "test_job",
			Status:     db.JobStatusRunning,
			RetryCount: 0,
			MaxRetries: 3,
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

	if store.retryCalled {
		t.Fatalf("did not expect retry to be called")
	}

	if store.deadCalled {
		t.Fatalf("did not expect dead to be called")
	}

	if store.completedParams.ID != jobID {
		t.Fatalf("expected completed id %v, got %v", jobID, store.completedParams.ID)
	}

	if store.completedParams.WorkerID != "worker-1" {
		t.Fatalf("expected worker-1, got %s", store.completedParams.WorkerID)
	}
}

func TestExecuteOnceSchedulesRetryWhenHandlerReturnsErrorAndRetriesRemain(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:         jobID,
			Kind:       "test_job",
			Status:     db.JobStatusRunning,
			RetryCount: 0,
			MaxRetries: 3,
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

	if !store.retryCalled {
		t.Fatalf("expected retry to be called")
	}

	if store.deadCalled {
		t.Fatalf("did not expect dead to be called")
	}

	if store.retryParams.ID != jobID {
		t.Fatalf("expected retry id %v, got %v", jobID, store.retryParams.ID)
	}

	if store.retryParams.WorkerID != "worker-1" {
		t.Fatalf("expected worker-1, got %s", store.retryParams.WorkerID)
	}

	if store.retryParams.ErrorMessage != "handler failed" {
		t.Fatalf("expected handler failed, got %s", store.retryParams.ErrorMessage)
	}

	if store.retryParams.DelaySeconds != 1 {
		t.Fatalf("expected delay 1, got %d", store.retryParams.DelaySeconds)
	}
}

func TestExecuteOnceMarksJobDeadWhenHandlerReturnsErrorAndRetriesExhausted(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:         jobID,
			Kind:       "test_job",
			Status:     db.JobStatusRunning,
			RetryCount: 2,
			MaxRetries: 3,
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

	if store.retryCalled {
		t.Fatalf("did not expect retry to be called")
	}

	if !store.deadCalled {
		t.Fatalf("expected dead to be called")
	}

	if store.deadParams.ID != jobID {
		t.Fatalf("expected dead id %v, got %v", jobID, store.deadParams.ID)
	}

	if store.deadParams.WorkerID != "worker-1" {
		t.Fatalf("expected worker-1, got %s", store.deadParams.WorkerID)
	}

	if store.deadParams.ErrorMessage != "handler failed" {
		t.Fatalf("expected handler failed, got %s", store.deadParams.ErrorMessage)
	}
}

func TestExecuteOnceSchedulesRetryWhenHandlerMissing(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:         jobID,
			Kind:       "unknown_job",
			Status:     db.JobStatusRunning,
			RetryCount: 0,
			MaxRetries: 3,
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

	if !store.retryCalled {
		t.Fatalf("expected retry to be called")
	}

	if store.deadCalled {
		t.Fatalf("did not expect dead to be called")
	}

	expected := "no handler registered for job kind: unknown_job"
	if store.retryParams.ErrorMessage != expected {
		t.Fatalf("expected %q, got %q", expected, store.retryParams.ErrorMessage)
	}
}

func TestExecuteOnceSchedulesRetryWhenHandlerPanics(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:         jobID,
			Kind:       "test_job",
			Status:     db.JobStatusRunning,
			RetryCount: 0,
			MaxRetries: 3,
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

	if !store.retryCalled {
		t.Fatalf("expected retry to be called")
	}

	if store.deadCalled {
		t.Fatalf("did not expect dead to be called")
	}

	expected := "handler panic: boom"
	if store.retryParams.ErrorMessage != expected {
		t.Fatalf("expected %q, got %q", expected, store.retryParams.ErrorMessage)
	}
}

func TestRetryDelaySeconds(t *testing.T) {
	tests := []struct {
		name              string
		currentRetryCount int32
		expected          int32
	}{
		{
			name:              "first retry",
			currentRetryCount: 0,
			expected:          1,
		},
		{
			name:              "second retry",
			currentRetryCount: 1,
			expected:          2,
		},
		{
			name:              "third retry",
			currentRetryCount: 2,
			expected:          4,
		},
		{
			name:              "caps at max delay",
			currentRetryCount: 10,
			expected:          60,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryDelaySeconds(tt.currentRetryCount)
			if got != tt.expected {
				t.Fatalf("expected %d, got %d", tt.expected, got)
			}
		})
	}
}

func TestExecuteOnceRenewsLeaseWhileHandlerRuns(t *testing.T) {
	jobID := testUUID()

	renewed := make(chan db.RenewJobLeaseParams, 1)

	store := &fakeStore{
		claimJob: db.Job{
			ID:         jobID,
			Kind:       "long_job",
			Status:     db.JobStatusRunning,
			RetryCount: 0,
			MaxRetries: 3,
		},
	}

	store.renewFunc = func(ctx context.Context, arg db.RenewJobLeaseParams) (db.Job, error) {
		select {
		case renewed <- arg:
		default:
		}

		return db.Job{
			ID:          arg.ID,
			Status:      db.JobStatusRunning,
			RetryCount:  store.claimJob.RetryCount,
			MaxRetries:  store.claimJob.MaxRetries,
			LockedUntil: pgtype.Timestamptz{Time: time.Now().Add(time.Duration(arg.LeaseSeconds) * time.Second), Valid: true},
		}, nil
	}

	handlerCanFinish := make(chan struct{})

	handler := NewHandlerFunc("long_job", func(ctx context.Context, job db.Job) error {
		select {
		case <-renewed:
			close(handlerCanFinish)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return errors.New("timed out waiting for heartbeat")
		}
	})

	registry, err := NewRegistry(handler)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	executor := NewExecutorWithHeartbeat(
		store,
		registry,
		"worker-1",
		30,
		10*time.Millisecond,
		nil,
	)

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

	if store.retryCalled {
		t.Fatalf("did not expect retry to be called")
	}

	if store.deadCalled {
		t.Fatalf("did not expect dead to be called")
	}

	select {
	case <-handlerCanFinish:
	default:
		t.Fatalf("expected handler to finish after heartbeat")
	}

	if !store.renewCalled {
		t.Fatalf("expected renew lease to be called")
	}

	if store.renewParams.ID != jobID {
		t.Fatalf("expected renewed job id %v, got %v", jobID, store.renewParams.ID)
	}

	if store.renewParams.WorkerID != "worker-1" {
		t.Fatalf("expected worker-1, got %s", store.renewParams.WorkerID)
	}

	if store.renewParams.LeaseSeconds != 30 {
		t.Fatalf("expected lease seconds 30, got %d", store.renewParams.LeaseSeconds)
	}
}

func TestExecuteOnceCancelsHandlerWhenLeaseRenewalFails(t *testing.T) {
	jobID := testUUID()

	store := &fakeStore{
		claimJob: db.Job{
			ID:         jobID,
			Kind:       "long_job",
			Status:     db.JobStatusRunning,
			RetryCount: 0,
			MaxRetries: 3,
		},
		renewErr: errors.New("database unavailable"),
	}

	handlerCancelled := make(chan struct{})

	handler := NewHandlerFunc("long_job", func(ctx context.Context, job db.Job) error {
		<-ctx.Done()
		close(handlerCancelled)
		return ctx.Err()
	})

	registry, err := NewRegistry(handler)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	executor := NewExecutorWithHeartbeat(
		store,
		registry,
		"worker-1",
		30,
		10*time.Millisecond,
		nil,
	)

	processed, err := executor.ExecuteOnce(context.Background())
	if err != nil {
		t.Fatalf("execute once: %v", err)
	}

	if !processed {
		t.Fatalf("expected processed=true")
	}

	select {
	case <-handlerCancelled:
	default:
		t.Fatalf("expected handler context to be cancelled")
	}

	if !store.renewCalled {
		t.Fatalf("expected renew lease to be called")
	}

	if store.completedCalled {
		t.Fatalf("did not expect completed to be called")
	}

	if !store.retryCalled {
		t.Fatalf("expected retry to be called")
	}

	if store.deadCalled {
		t.Fatalf("did not expect dead to be called")
	}
}