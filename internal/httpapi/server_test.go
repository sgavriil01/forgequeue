package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeHealthService struct {
	err error
}

func (f fakeHealthService) Ping(ctx context.Context) error {
	return f.err
}

func TestHealthzReturnsOK(t *testing.T) {
	server := NewServer(fakeHealthService{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestReadyzReturnsOKWhenDatabaseIsReady(t *testing.T) {
	server := NewServer(fakeHealthService{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
}

func TestReadyzReturnsServiceUnavailableWhenDatabaseIsNotReady(t *testing.T) {
	server := NewServer(fakeHealthService{err: errors.New("db down")}, nil)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, rec.Code)
	}
}