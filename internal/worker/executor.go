package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/jackc/pgx/v5"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
)

const (
	defaultErrorMessageLimit = 1000
	maxRetryDelaySeconds    = int32(60)
)

type Store interface {
	ClaimNextJob(ctx context.Context, arg db.ClaimNextJobParams) (db.Job, error)
	MarkJobCompleted(ctx context.Context, arg db.MarkJobCompletedParams) (db.Job, error)
	ScheduleJobRetry(ctx context.Context, arg db.ScheduleJobRetryParams) (db.Job, error)
	MarkJobDead(ctx context.Context, arg db.MarkJobDeadParams) (db.Job, error)
}

type Executor struct {
	store        Store
	registry     *Registry
	workerID     string
	leaseSeconds int32
	logger       *slog.Logger
}

func NewExecutor(store Store, registry *Registry, workerID string, leaseSeconds int32, logger *slog.Logger) *Executor {
	if logger == nil {
		logger = slog.Default()
	}

	return &Executor{
		store:        store,
		registry:     registry,
		workerID:     workerID,
		leaseSeconds: leaseSeconds,
		logger:       logger,
	}
}

func (e *Executor) ExecuteOnce(ctx context.Context) (bool, error) {
	job, err := e.store.ClaimNextJob(ctx, db.ClaimNextJobParams{
		WorkerID:     e.workerID,
		LeaseSeconds: e.leaseSeconds,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}

		return false, fmt.Errorf("claim job: %w", err)
	}

	e.logger.Info(
		"job claimed",
		"job_id", job.ID,
		"kind", job.Kind,
		"worker_id", e.workerID,
	)

	handler, ok := e.registry.HandlerFor(job.Kind)
	if !ok {
		message := fmt.Sprintf("no handler registered for job kind: %s", job.Kind)
		if err := e.handleFailure(ctx, job, message); err != nil {
			return true, err
		}

		return true, nil
	}

	if err := e.runHandler(ctx, handler, job); err != nil {
		if markErr := e.handleFailure(ctx, job, err.Error()); markErr != nil {
			return true, markErr
		}

		return true, nil
	}

	_, err = e.store.MarkJobCompleted(ctx, db.MarkJobCompletedParams{
		ID:       job.ID,
		WorkerID: e.workerID,
	})
	if err != nil {
		return true, fmt.Errorf("mark job completed: %w", err)
	}

	e.logger.Info(
		"job completed",
		"job_id", job.ID,
		"kind", job.Kind,
		"worker_id", e.workerID,
	)

	return true, nil
}

func (e *Executor) runHandler(ctx context.Context, handler JobHandler, job db.Job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			e.logger.Error(
				"job handler panicked",
				"job_id", job.ID,
				"kind", job.Kind,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)

			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()

	return handler.Handle(ctx, job)
}

func (e *Executor) handleFailure(ctx context.Context, job db.Job, message string) error {
	message = truncate(message, defaultErrorMessageLimit)

	nextRetryCount := job.RetryCount + 1

	if nextRetryCount >= job.MaxRetries {
		_, err := e.store.MarkJobDead(ctx, db.MarkJobDeadParams{
			ID:           job.ID,
			WorkerID:     e.workerID,
			ErrorMessage: message,
		})
		if err != nil {
			return fmt.Errorf("mark job dead: %w", err)
		}

		e.logger.Info(
			"job dead",
			"job_id", job.ID,
			"kind", job.Kind,
			"worker_id", e.workerID,
			"retry_count", nextRetryCount,
			"max_retries", job.MaxRetries,
			"error", message,
		)

		return nil
	}

	delaySeconds := retryDelaySeconds(job.RetryCount)

	_, err := e.store.ScheduleJobRetry(ctx, db.ScheduleJobRetryParams{
		ID:           job.ID,
		WorkerID:     e.workerID,
		ErrorMessage: message,
		DelaySeconds: delaySeconds,
	})
	if err != nil {
		return fmt.Errorf("schedule job retry: %w", err)
	}

	e.logger.Info(
		"job retry scheduled",
		"job_id", job.ID,
		"kind", job.Kind,
		"worker_id", e.workerID,
		"retry_count", nextRetryCount,
		"max_retries", job.MaxRetries,
		"delay_seconds", delaySeconds,
		"error", message,
	)

	return nil
}

func retryDelaySeconds(currentRetryCount int32) int32 {
	delay := int32(1)

	for i := int32(0); i < currentRetryCount; i++ {
		delay *= 2

		if delay >= maxRetryDelaySeconds {
			return maxRetryDelaySeconds
		}
	}

	return delay
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}

	return value[:limit]
}