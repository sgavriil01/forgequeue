package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	"github.com/sgavriil01/forgequeue/internal/jobs"
)

type JobService interface {
	Ping(ctx context.Context) error
	CreateJob(ctx context.Context, input jobs.CreateJobInput) (db.Job, error)
	GetJob(ctx context.Context, id pgtype.UUID) (db.Job, error)
	ListJobs(ctx context.Context, status *db.JobStatus, limit int32) ([]db.Job, error)
	CancelJob(ctx context.Context, id pgtype.UUID) (db.Job, error)
	CountJobsByStatus(ctx context.Context, status db.JobStatus) (int64, error)
}

type Server struct {
	jobService JobService
	logger     *slog.Logger
}

func NewServer(jobService JobService, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		jobService: jobService,
		logger:     logger,
	}
}

func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(logRequests(s.logger))
	r.Use(recoverPanic(s.logger))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	r.Post("/jobs", s.handleCreateJob)
	r.Get("/jobs/{id}", s.handleGetJob)
	r.Get("/jobs", s.handleListJobs)
	r.Post("/jobs/{id}/cancel", s.handleCancelJob)

	r.Get("/metrics", s.handleMetrics)

	return r
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.jobService == nil {
		writeError(w, http.StatusServiceUnavailable, "DATABASE_NOT_CONFIGURED", "database is not configured")
		return
	}

	if err := s.jobService.Ping(r.Context()); err != nil {
		s.logger.Error("readiness check failed", "error", err)

		writeError(w, http.StatusServiceUnavailable, "DATABASE_NOT_READY", "database is not ready")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ready",
	})
}
