package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	"github.com/sgavriil01/forgequeue/internal/jobs"
)

const maxRequestBodyBytes = 1 << 20 // 1 MB

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req createJobRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "request body must be valid JSON")
		return
	}

	job, err := s.jobService.CreateJob(r.Context(), jobs.CreateJobInput{
		Kind:       req.Kind,
		Payload:    req.Payload,
		RunAt:      req.RunAt,
		MaxRetries: req.MaxRetries,
	})
	if err != nil {
		switch {
		case errors.Is(err, jobs.ErrInvalidJobKind):
			writeError(w, http.StatusUnprocessableEntity, "INVALID_JOB_KIND", err.Error())
		case errors.Is(err, jobs.ErrInvalidPayload):
			writeError(w, http.StatusUnprocessableEntity, "INVALID_PAYLOAD", err.Error())
		case errors.Is(err, jobs.ErrInvalidRetries):
			writeError(w, http.StatusUnprocessableEntity, "INVALID_RETRIES", err.Error())
		default:
			s.logger.Error("create job failed", "error", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create job")
		}

		return
	}

	writeJSON(w, http.StatusCreated, toJobResponse(job))
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	rawID := chi.URLParam(r, "id")

	id, err := parseUUID(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JOB_ID", "job id must be a valid UUID")
		return
	}

	job, err := s.jobService.GetJob(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, jobs.ErrJobNotFound):
			writeError(w, http.StatusNotFound, "JOB_NOT_FOUND", "job not found")
		default:
			s.logger.Error("get job failed", "job_id", rawID, "error", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get job")
		}

		return
	}

	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	var status *db.JobStatus

	rawStatus := query.Get("status")
	if rawStatus != "" {
		parsedStatus, err := jobs.ParseJobStatus(rawStatus)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_STATUS", "status must be one of: pending, running, completed, failed, dead, cancelled")
			return
		}

		status = &parsedStatus
	}

	limit := int32(50)

	rawLimit := query.Get("limit")
	if rawLimit != "" {
		parsedLimit, err := strconv.ParseInt(rawLimit, 10, 32)
		if err != nil || parsedLimit <= 0 {
			writeError(w, http.StatusBadRequest, "INVALID_LIMIT", "limit must be a positive integer")
			return
		}

		limit = int32(parsedLimit)
	}

	jobsList, err := s.jobService.ListJobs(r.Context(), status, limit)
	if err != nil {
		s.logger.Error("list jobs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list jobs")
		return
	}

	writeJSON(w, http.StatusOK, toJobResponses(jobsList))
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	rawID := chi.URLParam(r, "id")

	id, err := parseUUID(rawID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JOB_ID", "job id must be a valid UUID")
		return
	}

	job, err := s.jobService.CancelJob(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, jobs.ErrJobNotCancelable):
			writeError(w, http.StatusConflict, "JOB_NOT_CANCELABLE", "job is not cancelable")
		default:
			s.logger.Error("cancel job failed", "job_id", rawID, "error", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to cancel job")
		}

		return
	}

	writeJSON(w, http.StatusOK, toJobResponse(job))
}