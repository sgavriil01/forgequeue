package httpapi

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
)

type createJobRequest struct {
	Kind       string          `json:"kind"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	RunAt      *time.Time      `json:"run_at,omitempty"`
	MaxRetries *int32          `json:"max_retries,omitempty"`
}

type jobResponse struct {
	ID           string          `json:"id"`
	Kind         string          `json:"kind"`
	Status       db.JobStatus    `json:"status"`
	Payload      json.RawMessage `json:"payload"`
	Priority     int32           `json:"priority"`
	RunAt        any             `json:"run_at"`
	CreatedAt    any             `json:"created_at"`
	UpdatedAt    any             `json:"updated_at"`
	RetryCount   int32           `json:"retry_count"`
	MaxRetries   int32           `json:"max_retries"`
	ErrorMessage any             `json:"error_message,omitempty"`
}

func toJobResponse(job db.Job) jobResponse {
	return jobResponse{
		ID:           uuidToString(job.ID),
		Kind:         job.Kind,
		Status:       job.Status,
		Payload:      json.RawMessage(job.Payload),
		Priority:     job.Priority,
		RunAt:        job.RunAt,
		CreatedAt:    job.CreatedAt,
		UpdatedAt:    job.UpdatedAt,
		RetryCount:   job.RetryCount,
		MaxRetries:   job.MaxRetries,
		ErrorMessage: job.ErrorMessage,
	}
}

func toJobResponses(jobs []db.Job) []jobResponse {
	responses := make([]jobResponse, 0, len(jobs))

	for _, job := range jobs {
		responses = append(responses, toJobResponse(job))
	}

	return responses
}

func parseUUID(raw string) (pgtype.UUID, error) {
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return pgtype.UUID{}, err
	}

	return pgtype.UUID{
		Bytes: [16]byte(parsed),
		Valid: true,
	}, nil
}

func uuidToString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}

	b := id.Bytes

	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	)
}