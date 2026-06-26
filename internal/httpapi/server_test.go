package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	"github.com/sgavriil01/forgequeue/internal/jobs"
)

type fakeJobService struct {
	pingErr error
}

func (f fakeJobService) Ping(ctx context.Context) error {
	return f.pingErr
}

func (f fakeJobService) CreateJob(ctx context.Context, input jobs.CreateJobInput) (db.Job, error) {
	return db.Job{}, nil
}

func (f fakeJobService) GetJob(ctx context.Context, id pgtype.UUID) (db.Job, error) {
	return db.Job{}, nil
}

func TestHealthzReturnsOK(t *testing.T) {
	server := NewServer(fakeJobService{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestReadyzReturnsOKWhenDatabaseIsReady(t *testing.T) {
	server := NewServer(fakeJobService{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestReadyzReturnsServiceUnavailableWhenDatabaseIsNotReady(t *testing.T) {
	server := NewServer(fakeJobService{pingErr: errors.New("db down")}, nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}