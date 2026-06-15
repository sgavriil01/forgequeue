package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type HealthService interface {
	Ping(ctx context.Context) error
}

type Server struct {
	healthService HealthService
	logger        *slog.Logger
}

func NewServer(healthService HealthService, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		healthService: healthService,
		logger:        logger,
	}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	return r
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.healthService == nil {
		writeError(w, http.StatusServiceUnavailable, "DATABASE_NOT_CONFIGURED", "database is not configured")
		return
	}

	if err := s.healthService.Ping(r.Context()); err != nil {
		s.logger.Error("readiness check failed", "error", err)

		writeError(w, http.StatusServiceUnavailable, "DATABASE_NOT_READY", "database is not ready")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ready",
	})
}