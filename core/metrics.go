package core

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the five Prometheus collectors exported by the notasap library.
// Callers pass a prometheus.Registerer via QueueConfig.Registerer. If the registerer
// is nil the collectors are still constructed (so .Inc / .Observe calls remain safe)
// but they are not registered — nothing will scrape them.
type Metrics struct {
	Enqueued  *prometheus.CounterVec   // labels: job_type
	Processed *prometheus.CounterVec   // labels: job_type, status
	Duration  *prometheus.HistogramVec // labels: job_type
	InFlight  prometheus.Gauge
	Poll      prometheus.Histogram
}

// NewMetrics builds the five notasap collectors. When reg is non-nil the collectors
// are registered on it; when reg is nil they are constructed but unregistered.
// In both cases the returned Metrics is safe to call — this removes nil-checks from
// the hot path in queue/worker.go.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_jobs_enqueued_total",
			Help: "Number of jobs successfully inserted into notasap_jobs.",
		}, []string{"job_type"}),

		Processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_jobs_processed_total",
			Help: "Number of jobs that reached a terminal status (done/failed/cancelled/exhausted).",
		}, []string{"job_type", "status"}),

		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notasap_job_duration_seconds",
			Help:    "Per-attempt handler execution time, from worker claim to handler return.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms .. 16s
		}, []string{"job_type"}),

		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "notasap_jobs_in_flight",
			Help: "Number of jobs currently executing in worker goroutines.",
		}),

		Poll: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "notasap_poll_duration_seconds",
			Help:    "Time spent in a single ClaimJob round-trip to Postgres.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12), // 0.5ms .. 2s
		}),
	}
	if reg != nil {
		reg.MustRegister(m.Enqueued, m.Processed, m.Duration, m.InFlight, m.Poll)
	}
	return m
}
