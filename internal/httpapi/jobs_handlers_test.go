package httpapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	"github.com/sgavriil01/forgequeue/internal/jobs"
)

type fakeEndpointJobService struct {
	createJob db.Job
	createErr error

	getJob db.Job
	getErr error

	listJobs []db.Job
	listErr  error

	cancelJob db.Job
	cancelErr error
}

func (f fakeEndpointJobService) Ping(ctx context.Context) error {
	return nil
}

func (f fakeEndpointJobService) CreateJob(ctx context.Context, input jobs.CreateJobInput) (db.Job, error) {
	if f.createErr != nil {
		return db.Job{}, f.createErr
	}

	return f.createJob, nil
}

func (f fakeEndpointJobService) GetJob(ctx context.Context, id pgtype.UUID) (db.Job, error) {
	if f.getErr != nil {
		return db.Job{}, f.getErr
	}

	return f.getJob, nil
}

func (f fakeEndpointJobService) ListJobs(ctx context.Context, status *db.JobStatus, limit int32) ([]db.Job, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}

	return f.listJobs, nil
}

func (f fakeEndpointJobService) CancelJob(ctx context.Context, id pgtype.UUID) (db.Job, error) {
	if f.cancelErr != nil {
		return db.Job{}, f.cancelErr
	}

	return f.cancelJob, nil
}

func (f fakeEndpointJobService) CountJobsByStatus(ctx context.Context, status db.JobStatus) (int64, error) {
	return 0, nil
}

func TestCreateJobReturnsCreated(t *testing.T) {
	id, err := parseUUID("11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}

	server := NewServer(fakeEndpointJobService{
		createJob: db.Job{
			ID:         id,
			Kind:       "test_job",
			Status:     db.JobStatusPending,
			Payload:    []byte(`{"message":"hello"}`),
			MaxRetries: 3,
		},
	}, nil)

	body := bytes.NewBufferString(`{"kind":"test_job","payload":{"message":"hello"},"max_retries":3}`)
	req := httptest.NewRequest(http.MethodPost, "/jobs", body)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
}

func TestCreateJobReturnsBadRequestForInvalidJSON(t *testing.T) {
	server := NewServer(fakeEndpointJobService{}, nil)

	body := bytes.NewBufferString(`{"kind":`)
	req := httptest.NewRequest(http.MethodPost, "/jobs", body)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestCreateJobReturnsValidationError(t *testing.T) {
	server := NewServer(fakeEndpointJobService{
		createErr: jobs.ErrInvalidJobKind,
	}, nil)

	body := bytes.NewBufferString(`{"kind":"","payload":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/jobs", body)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected status %d, got %d", http.StatusUnprocessableEntity, rec.Code)
	}
}

func TestGetJobReturnsOK(t *testing.T) {
	id, err := parseUUID("11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}

	server := NewServer(fakeEndpointJobService{
		getJob: db.Job{
			ID:         id,
			Kind:       "test_job",
			Status:     db.JobStatusPending,
			Payload:    []byte(`{}`),
			MaxRetries: 3,
		},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs/11111111-1111-1111-1111-111111111111", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestGetJobReturnsBadRequestForInvalidUUID(t *testing.T) {
	server := NewServer(fakeEndpointJobService{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs/not-a-uuid", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestGetJobReturnsNotFound(t *testing.T) {
	server := NewServer(fakeEndpointJobService{
		getErr: jobs.ErrJobNotFound,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs/11111111-1111-1111-1111-111111111111", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, rec.Code)
	}
}

func TestGetJobReturnsInternalError(t *testing.T) {
	server := NewServer(fakeEndpointJobService{
		getErr: errors.New("db exploded"),
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs/11111111-1111-1111-1111-111111111111", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rec.Code)
	}
}

func TestListJobsReturnsOK(t *testing.T) {
	id, err := parseUUID("11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}

	server := NewServer(fakeEndpointJobService{
		listJobs: []db.Job{
			{
				ID:         id,
				Kind:       "test_job",
				Status:     db.JobStatusPending,
				Payload:    []byte(`{}`),
				MaxRetries: 3,
			},
		},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs?status=pending&limit=10", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestListJobsReturnsBadRequestForInvalidStatus(t *testing.T) {
	server := NewServer(fakeEndpointJobService{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs?status=unknown", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestListJobsReturnsBadRequestForInvalidLimit(t *testing.T) {
	server := NewServer(fakeEndpointJobService{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=abc", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestListJobsReturnsInternalError(t *testing.T) {
	server := NewServer(fakeEndpointJobService{
		listErr: errors.New("db exploded"),
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rec.Code)
	}
}

func TestCancelJobReturnsOK(t *testing.T) {
	id, err := parseUUID("11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatalf("parse uuid: %v", err)
	}

	server := NewServer(fakeEndpointJobService{
		cancelJob: db.Job{
			ID:         id,
			Kind:       "test_job",
			Status:     db.JobStatusCancelled,
			Payload:    []byte(`{}`),
			MaxRetries: 3,
		},
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/jobs/11111111-1111-1111-1111-111111111111/cancel", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestCancelJobReturnsBadRequestForInvalidUUID(t *testing.T) {
	server := NewServer(fakeEndpointJobService{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/jobs/not-a-uuid/cancel", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
}

func TestCancelJobReturnsConflictWhenNotCancelable(t *testing.T) {
	server := NewServer(fakeEndpointJobService{
		cancelErr: jobs.ErrJobNotCancelable,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/jobs/11111111-1111-1111-1111-111111111111/cancel", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d", http.StatusConflict, rec.Code)
	}
}

func TestCancelJobReturnsInternalError(t *testing.T) {
	server := NewServer(fakeEndpointJobService{
		cancelErr: errors.New("db exploded"),
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/jobs/11111111-1111-1111-1111-111111111111/cancel", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, rec.Code)
	}
}