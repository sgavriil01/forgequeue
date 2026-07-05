package httpapi

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
	fqmetrics "github.com/sgavriil01/forgequeue/internal/metrics"
)

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	statuses := []db.JobStatus{
		db.JobStatusPending,
		db.JobStatusRunning,
		db.JobStatusCompleted,
		db.JobStatusFailed,
		db.JobStatusDead,
		db.JobStatusCancelled,
	}

	for _, status := range statuses {
		count, err := s.jobService.CountJobsByStatus(r.Context(), status)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "METRICS_UNAVAILABLE", "failed to collect queue metrics")
			return
		}

		fqmetrics.SetJobsByStatus(string(status), count)
	}

	promhttp.Handler().ServeHTTP(w, r)
}