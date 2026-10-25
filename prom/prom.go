// Package prom provides a Prometheus implementation of notasap.Observer.
package prom

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	notasap "github.com/nottolstybai/not-asap"
)

// Metrics exports notasap job metrics to Prometheus. Create it with New and
// pass it to notasap.WithObserver.
type Metrics struct {
	scheduled    *prometheus.CounterVec
	finished     *prometheus.CounterVec
	retried      *prometheus.CounterVec
	jobDuration  *prometheus.HistogramVec
	running      *prometheus.GaugeVec
	pollDuration *prometheus.HistogramVec
	rescued      *prometheus.CounterVec
	stuck        *prometheus.CounterVec
	deleted      *prometheus.CounterVec
}

var _ notasap.Observer = (*Metrics)(nil)

type config struct {
	jobBuckets  []float64
	pollBuckets []float64
}

// Option configures New. Nil options are skipped.
type Option func(*config)

// WithJobDurationBuckets overrides the buckets of notasap_job_duration_seconds.
// An empty slice makes New return an error.
func WithJobDurationBuckets(b []float64) Option {
	return func(c *config) { c.jobBuckets = slices.Clone(b) }
}

// WithPollDurationBuckets overrides the buckets of notasap_poll_duration_seconds.
// An empty slice makes New return an error.
func WithPollDurationBuckets(b []float64) Option {
	return func(c *config) { c.pollBuckets = slices.Clone(b) }
}

// New creates the metrics and registers them with reg. If registration fails,
// collectors registered so far are unregistered and the error is returned.
func New(reg prometheus.Registerer, opts ...Option) (*Metrics, error) {
	if reg == nil {
		return nil, errors.New("notasap/prom: nil Registerer")
	}
	cfg := config{
		jobBuckets:  prometheus.ExponentialBuckets(0.005, 2, 15),
		pollBuckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if err := checkBuckets("WithJobDurationBuckets", cfg.jobBuckets); err != nil {
		return nil, err
	}
	if err := checkBuckets("WithPollDurationBuckets", cfg.pollBuckets); err != nil {
		return nil, err
	}

	m := &Metrics{
		scheduled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_jobs_scheduled_total", Help: "Jobs scheduled.",
		}, []string{"kind"}),
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_jobs_finished_total", Help: "Jobs that reached a terminal status.",
		}, []string{"kind", "status"}),
		retried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_jobs_retried_total", Help: "Job attempts sent back for another try.",
		}, []string{"kind", "reason"}),
		jobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "notasap_job_duration_seconds", Help: "Job attempt duration.", Buckets: cfg.jobBuckets,
		}, []string{"kind", "outcome"}),
		running: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notasap_jobs_running", Help: "Jobs currently executing.",
		}, []string{"kind"}),
		pollDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "notasap_poll_duration_seconds", Help: "Duration of a poll for jobs.", Buckets: cfg.pollBuckets,
		}, []string{"kind"}),
		rescued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_jobs_rescued_total", Help: "Stuck jobs returned to the queue.",
		}, []string{"kind"}),
		stuck: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_jobs_stuck_total", Help: "Jobs found stuck.",
		}, []string{"kind"}),
		deleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notasap_janitor_deleted_total", Help: "Jobs deleted by the janitor.",
		}, []string{"status"}),
	}

	cs := []prometheus.Collector{
		m.scheduled, m.finished, m.retried, m.jobDuration, m.running,
		m.pollDuration, m.rescued, m.stuck, m.deleted,
	}
	for i, c := range cs {
		if err := reg.Register(c); err != nil {
			for _, done := range cs[:i] {
				reg.Unregister(done)
			}
			return nil, err
		}
	}
	return m, nil
}

// checkBuckets rejects buckets client_golang would panic on at first use.
func checkBuckets(opt string, b []float64) error {
	if len(b) == 0 {
		return fmt.Errorf("notasap/prom: %s: buckets must not be empty", opt)
	}
	for i, v := range b {
		if math.IsNaN(v) {
			return fmt.Errorf("notasap/prom: %s: bucket %d is NaN", opt, i)
		}
		if i > 0 && v <= b[i-1] {
			return fmt.Errorf("notasap/prom: %s: buckets must be strictly increasing", opt)
		}
	}
	return nil
}

// JobScheduled implements notasap.Observer.
func (m *Metrics) JobScheduled(kind string) { m.scheduled.WithLabelValues(kind).Inc() }

// JobStarted implements notasap.Observer.
func (m *Metrics) JobStarted(kind string) { m.running.WithLabelValues(kind).Inc() }

// JobEnded implements notasap.Observer.
func (m *Metrics) JobEnded(kind string) { m.running.WithLabelValues(kind).Dec() }

// JobFinished implements notasap.Observer.
func (m *Metrics) JobFinished(kind string, status notasap.Status, took time.Duration) {
	s := status.String()
	m.finished.WithLabelValues(kind, s).Inc()
	m.jobDuration.WithLabelValues(kind, s).Observe(took.Seconds())
}

// JobRetried implements notasap.Observer.
func (m *Metrics) JobRetried(kind string, reason notasap.RetryReason, took time.Duration) {
	m.retried.WithLabelValues(kind, string(reason)).Inc()
	m.jobDuration.WithLabelValues(kind, "retried").Observe(took.Seconds())
}

// JobStuck implements notasap.Observer.
func (m *Metrics) JobStuck(kind string) { m.stuck.WithLabelValues(kind).Inc() }

// JobRescued implements notasap.Observer.
func (m *Metrics) JobRescued(kind string) { m.rescued.WithLabelValues(kind).Inc() }

// Polled implements notasap.Observer.
func (m *Metrics) Polled(kind string, took time.Duration) {
	m.pollDuration.WithLabelValues(kind).Observe(took.Seconds())
}

// JanitorDeleted implements notasap.Observer.
func (m *Metrics) JanitorDeleted(status notasap.Status, n int64) {
	m.deleted.WithLabelValues(status.String()).Add(float64(n))
}
