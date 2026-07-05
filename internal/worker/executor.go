package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	"github.com/sgavriil01/forgequeue/internal/metrics"
)

const (
	defaultErrorMessageLimit = 1000
	maxRetryDelaySeconds    = int32(60)
	minHeartbeatInterval    = 100 * time.Millisecond
)

type Store interface {
	ClaimNextJob(ctx context.Context, arg db.ClaimNextJobParams) (db.Job, error)
	MarkJobCompleted(ctx context.Context, arg db.MarkJobCompletedParams) (db.Job, error)
	ScheduleJobRetry(ctx context.Context, arg db.ScheduleJobRetryParams) (db.Job, error)
	MarkJobDead(ctx context.Context, arg db.MarkJobDeadParams) (db.Job, error)
	RenewJobLease(ctx context.Context, arg db.RenewJobLeaseParams) (db.Job, error)
}

type Executor struct {
	store             Store
	registry          *Registry
	workerID          string
	leaseSeconds      int32
	heartbeatInterval time.Duration
	logger            *slog.Logger
}

func NewExecutor(store Store, registry *Registry, workerID string, leaseSeconds int32, logger *slog.Logger) *Executor {
	return NewExecutorWithHeartbeat(
		store,
		registry,
		workerID,
		leaseSeconds,
		defaultHeartbeatInterval(leaseSeconds),
		logger,
	)
}

func NewExecutorWithHeartbeat(
	store Store,
	registry *Registry,
	workerID string,
	leaseSeconds int32,
	heartbeatInterval time.Duration,
	logger *slog.Logger,
) *Executor {
	if logger == nil {
		logger = slog.Default()
	}

	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval(leaseSeconds)
	}

	return &Executor{
		store:             store,
		registry:          registry,
		workerID:          workerID,
		leaseSeconds:      leaseSeconds,
		heartbeatInterval: heartbeatInterval,
		logger:            logger,
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

	startedAt := time.Now()

	queueLatency := time.Duration(0)
	if job.CreatedAt.Valid {
		queueLatency = time.Since(job.CreatedAt.Time)
	}

	metrics.RecordJobStarted(job.Kind, queueLatency)
	defer metrics.RecordJobFinished()

	e.logger.Info(
		"job claimed",
		"job_id", job.ID,
		"worker_id", e.workerID,
		"kind", job.Kind,
		"queue_latency", queueLatency.String(),
	)

	handler, ok := e.registry.HandlerFor(job.Kind)
	if !ok {
		message := fmt.Sprintf("no handler registered for job kind: %s", job.Kind)
		if err := e.handleFailure(ctx, job, message, startedAt); err != nil {
			return true, err
		}

		return true, nil
	}

	handlerCtx, stopHeartbeat := e.startHeartbeat(ctx, job)

	handlerErr := e.runHandler(handlerCtx, handler, job)

	stopHeartbeat()

	if handlerErr != nil {
		if markErr := e.handleFailure(ctx, job, handlerErr.Error(), startedAt); markErr != nil {
			return true, markErr
		}

		return true, nil
	}

	_, err = e.store.MarkJobCompleted(ctx, db.MarkJobCompletedParams{
		ID:       job.ID,
		WorkerID: e.workerID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			e.logger.Warn(
				"job lease lost before completion",
				"job_id", job.ID,
				"worker_id", e.workerID,
				"kind", job.Kind,
			)

			return true, nil
		}

		return true, fmt.Errorf("mark job completed: %w", err)
	}

	duration := time.Since(startedAt)

	e.logger.Info(
		"job completed",
		"job_id", job.ID,
		"worker_id", e.workerID,
		"kind", job.Kind,
		"duration", duration.String(),
	)

	metrics.RecordJobCompleted(job.Kind, duration)

	return true, nil
}

func (e *Executor) startHeartbeat(ctx context.Context, job db.Job) (context.Context, func()) {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(e.heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_, err := e.store.RenewJobLease(heartbeatCtx, db.RenewJobLeaseParams{
					ID:           job.ID,
					WorkerID:     e.workerID,
					LeaseSeconds: e.leaseSeconds,
				})
				if err != nil {
					e.logger.Error(
						"job lease renewal failed",
						"job_id", job.ID,
						"worker_id", e.workerID,
						"kind", job.Kind,
						"error", err,
					)

					cancel()
					return
				}

				e.logger.Debug(
					"job lease renewed",
					"job_id", job.ID,
					"worker_id", e.workerID,
					"kind", job.Kind,
				)

			case <-heartbeatCtx.Done():
				return
			}
		}
	}()

	stop := func() {
		cancel()
		<-done
	}

	return heartbeatCtx, stop
}

func (e *Executor) runHandler(ctx context.Context, handler JobHandler, job db.Job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			e.logger.Error(
				"job handler panicked",
				"job_id", job.ID,
				"worker_id", e.workerID,
				"kind", job.Kind,
				"panic", recovered,
				"stack", string(debug.Stack()),
			)

			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()

	return handler.Handle(ctx, job)
}

func (e *Executor) handleFailure(ctx context.Context, job db.Job, message string, startedAt time.Time) error {
	message = truncate(message, defaultErrorMessageLimit)

	nextRetryCount := job.RetryCount + 1

	if nextRetryCount >= job.MaxRetries {
		_, err := e.store.MarkJobDead(ctx, db.MarkJobDeadParams{
			ID:           job.ID,
			WorkerID:     e.workerID,
			ErrorMessage: message,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				e.logger.Warn(
					"job lease lost before marking dead",
					"job_id", job.ID,
					"worker_id", e.workerID,
					"kind", job.Kind,
				)

				return nil
			}

			return fmt.Errorf("mark job dead: %w", err)
		}

		duration := time.Since(startedAt)

		e.logger.Error(
			"job moved to dead state",
			"job_id", job.ID,
			"worker_id", e.workerID,
			"kind", job.Kind,
			"retry_count", nextRetryCount,
			"max_retries", job.MaxRetries,
			"error", message,
			"duration", duration.String(),
		)

		metrics.RecordJobDead(job.Kind, duration)

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
		if errors.Is(err, pgx.ErrNoRows) {
			e.logger.Warn(
				"job lease lost before scheduling retry",
				"job_id", job.ID,
				"worker_id", e.workerID,
				"kind", job.Kind,
			)

			return nil
		}

		return fmt.Errorf("schedule job retry: %w", err)
	}

	duration := time.Since(startedAt)

	e.logger.Warn(
		"job scheduled for retry",
		"job_id", job.ID,
		"worker_id", e.workerID,
		"kind", job.Kind,
		"retry_count", nextRetryCount,
		"max_retries", job.MaxRetries,
		"delay_seconds", delaySeconds,
		"error", message,
		"duration", duration.String(),
	)

	metrics.RecordJobRetried(job.Kind, duration)

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

func defaultHeartbeatInterval(leaseSeconds int32) time.Duration {
	if leaseSeconds <= 0 {
		return time.Second
	}

	interval := time.Duration(leaseSeconds) * time.Second / 3
	if interval < minHeartbeatInterval {
		return minHeartbeatInterval
	}

	return interval
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}

	return value[:limit]
}