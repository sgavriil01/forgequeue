package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	jobsCompletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forgequeue_jobs_completed_total",
			Help: "Total number of jobs completed successfully.",
		},
		[]string{"kind"},
	)

	jobsRetriedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forgequeue_jobs_retried_total",
			Help: "Total number of jobs scheduled for retry.",
		},
		[]string{"kind"},
	)

	jobsDeadTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forgequeue_jobs_dead_total",
			Help: "Total number of jobs moved to dead state.",
		},
		[]string{"kind"},
	)

	jobsReclaimedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forgequeue_jobs_reclaimed_total",
			Help: "Total number of expired-lease jobs reclaimed.",
		},
		[]string{"kind", "status"},
	)

	jobDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "forgequeue_job_duration_seconds",
			Help:    "Duration of job handler execution.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"kind", "result"},
	)
)

func init() {
	prometheus.MustRegister(
		jobsCompletedTotal,
		jobsRetriedTotal,
		jobsDeadTotal,
		jobsReclaimedTotal,
		jobDurationSeconds,
	)
}

func RecordJobCompleted(kind string, duration time.Duration) {
	jobsCompletedTotal.WithLabelValues(kind).Inc()
	jobDurationSeconds.WithLabelValues(kind, "completed").Observe(duration.Seconds())
}

func RecordJobRetried(kind string, duration time.Duration) {
	jobsRetriedTotal.WithLabelValues(kind).Inc()
	jobDurationSeconds.WithLabelValues(kind, "retried").Observe(duration.Seconds())
}

func RecordJobDead(kind string, duration time.Duration) {
	jobsDeadTotal.WithLabelValues(kind).Inc()
	jobDurationSeconds.WithLabelValues(kind, "dead").Observe(duration.Seconds())
}

func RecordJobReclaimed(kind string, status string) {
	jobsReclaimedTotal.WithLabelValues(kind, status).Inc()
}