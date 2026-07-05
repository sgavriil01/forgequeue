package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var durationBuckets = []float64{
	0.1,
	0.5,
	1,
	5,
	10,
	30,
	60,
	120,
}

var (
	jobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forgequeue_jobs_total",
			Help: "Total number of jobs reaching a final state.",
		},
		[]string{"kind", "status"},
	)

	jobsCompletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forgequeue_jobs_completed_total",
			Help: "Total number of jobs completed successfully.",
		},
		[]string{"kind"},
	)

	retryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forgequeue_retry_total",
			Help: "Total number of job retry attempts.",
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
			Buckets: durationBuckets,
		},
		[]string{"kind", "result"},
	)

	queueLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "forgequeue_queue_latency_seconds",
			Help:    "Duration from job creation to first claim.",
			Buckets: durationBuckets,
		},
		[]string{"kind"},
	)

	activeWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "forgequeue_active_workers",
			Help: "Number of workers currently executing jobs.",
		},
	)

	queueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "forgequeue_queue_depth",
			Help: "Current number of jobs by status.",
		},
		[]string{"status"},
	)

	jobsByStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "forgequeue_jobs_by_status",
			Help: "Current number of jobs by status.",
		},
		[]string{"status"},
	)
)

func init() {
	prometheus.MustRegister(
		jobsTotal,
		jobsCompletedTotal,
		retryTotal,
		jobsRetriedTotal,
		jobsDeadTotal,
		jobsReclaimedTotal,
		jobDurationSeconds,
		queueLatencySeconds,
		activeWorkers,
		queueDepth,
		jobsByStatus,
	)

	initializeZeroValues()
}

func initializeZeroValues() {
	knownKinds := []string{
		"test_job",
	}

	finalStatuses := []string{
		"completed",
		"dead",
		"failed",
		"cancelled",
	}

	for _, kind := range knownKinds {
		for _, status := range finalStatuses {
			jobsTotal.WithLabelValues(kind, status).Add(0)
		}

		jobsCompletedTotal.WithLabelValues(kind).Add(0)
		retryTotal.WithLabelValues(kind).Add(0)
		jobsRetriedTotal.WithLabelValues(kind).Add(0)
		jobsDeadTotal.WithLabelValues(kind).Add(0)
	}

	statuses := []string{
		"pending",
		"running",
		"completed",
		"failed",
		"dead",
		"cancelled",
	}

	for _, status := range statuses {
		queueDepth.WithLabelValues(status).Set(0)
		jobsByStatus.WithLabelValues(status).Set(0)
	}
}

func RecordJobStarted(kind string, queueLatency time.Duration) {
	activeWorkers.Inc()

	if queueLatency >= 0 {
		queueLatencySeconds.WithLabelValues(kind).Observe(queueLatency.Seconds())
	}
}

func RecordJobFinished() {
	activeWorkers.Dec()
}

func RecordJobCompleted(kind string, duration time.Duration) {
	jobsTotal.WithLabelValues(kind, "completed").Inc()
	jobsCompletedTotal.WithLabelValues(kind).Inc()
	jobDurationSeconds.WithLabelValues(kind, "completed").Observe(duration.Seconds())
}

func RecordJobRetried(kind string, duration time.Duration) {
	retryTotal.WithLabelValues(kind).Inc()
	jobsRetriedTotal.WithLabelValues(kind).Inc()
	jobDurationSeconds.WithLabelValues(kind, "retried").Observe(duration.Seconds())
}

func RecordJobDead(kind string, duration time.Duration) {
	jobsTotal.WithLabelValues(kind, "dead").Inc()
	jobsDeadTotal.WithLabelValues(kind).Inc()
	jobDurationSeconds.WithLabelValues(kind, "dead").Observe(duration.Seconds())
}

func RecordJobReclaimed(kind string, status string) {
	jobsReclaimedTotal.WithLabelValues(kind, status).Inc()
}

func SetJobsByStatus(status string, count int64) {
	queueDepth.WithLabelValues(status).Set(float64(count))
	jobsByStatus.WithLabelValues(status).Set(float64(count))
}