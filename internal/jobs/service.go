package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrJobNotFound      = errors.New("job not found")
	ErrJobNotCancelable = errors.New("job is not cancelable")
	ErrInvalidJobKind   = errors.New("job kind is required")
	ErrInvalidPayload   = errors.New("payload must be a valid JSON object")
	ErrInvalidRetries   = errors.New("max_retries must be between 0 and 10")
	ErrInvalidStatus    = errors.New("invalid job status")
)

const (
	defaultMaxRetries = int32(3)
	defaultListLimit  = int32(50)
	maxListLimit      = int32(100)
)

type Service struct {
	queries *db.Queries
}

func NewService(queries *db.Queries) *Service {
	return &Service{
		queries: queries,
	}
}

type CreateJobInput struct {
	Kind       string
	Payload    json.RawMessage
	RunAt      *time.Time
	MaxRetries *int32
}

func (s *Service) CreateJob(ctx context.Context, input CreateJobInput) (db.Job, error) {
	kind := strings.TrimSpace(input.Kind)
	if kind == "" {
		return db.Job{}, ErrInvalidJobKind
	}

	payload, err := normalizePayload(input.Payload)
	if err != nil {
		return db.Job{}, err
	}

	runAt := pgtype.Timestamptz{
		Valid: false,
	}

	if input.RunAt != nil {
		runAt = pgtype.Timestamptz{
			Time: *input.RunAt,
			Valid: false,
		}
	}

	maxRetries := defaultMaxRetries
	if input.MaxRetries != nil {
		maxRetries = *input.MaxRetries
	}

	if maxRetries < 0 || maxRetries > 10 {
		return db.Job{}, ErrInvalidRetries
	}

	return s.queries.CreateJob(ctx, db.CreateJobParams{
		Kind: kind,
		Payload: payload,
		RunAt: runAt,
		MaxRetries: maxRetries,
	})
}

func (s *Service) GetJob(ctx context.Context, id pgtype.UUID) (db.Job, error) {
	job, err := s.queries.GetJobByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.Job{}, ErrJobNotFound
		}

		return db.Job{}, err
	}

	return job, nil
}

func (s *Service) ListJobs(ctx context.Context, status *db.JobStatus, limit int32) ([]db.Job, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}

	if limit > maxListLimit {
		limit = maxListLimit
	}

	if status == nil {
		return s.queries.ListJobs(ctx, limit)
	}

	return s.queries.ListJobsByStatus(ctx, db.ListJobsByStatusParams{
		Status: *status,
		JobLimit: limit,
	})
}

func (s *Service) CancelJob(ctx context.Context, id pgtype.UUID) (db.Job, error) {
	job, err := s.queries.CancelPendingJob(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return db.Job{}, ErrJobNotCancelable
		}

		return db.Job{}, err
	}

	return job, nil
}

func (s *Service) Ping(ctx context.Context) error {
	_, err := s.queries.Ping(ctx)
	return err
}

func ParseJobStatus(raw string) (db.JobStatus, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "pending":
		return db.JobStatusPending, nil
	case "running":
		return db.JobStatusRunning, nil
	case "completed":
		return db.JobStatusCompleted, nil
	case "failed":
		return db.JobStatusFailed, nil
	case "dead":
		return db.JobStatusDead, nil
	case "cancelled":
		return db.JobStatusCancelled, nil
	default:
		return "", ErrInvalidStatus
	}
}

func normalizePayload(payload json.RawMessage) ([]byte, error) {
	if len(payload) == 0 {
		return []byte(`{}`), nil
	}

	if !json.Valid(payload) {
		return nil, ErrInvalidPayload
	}

	trimmed := strings.TrimSpace(string(payload))
	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return nil, ErrInvalidPayload
	}

	return payload, nil
}