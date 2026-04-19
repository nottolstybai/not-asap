package core

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics_NilRegisterer_DoesNotPanic(t *testing.T) {
	m := NewMetrics(nil)
	require.NotNil(t, m)

	// All method calls must be safe even without a registry attached.
	assert.NotPanics(t, func() {
		m.Enqueued.WithLabelValues("send_email").Inc()
		m.Processed.WithLabelValues("send_email", "done").Inc()
		m.Duration.WithLabelValues("send_email").Observe(0.123)
		m.InFlight.Inc()
		m.InFlight.Dec()
		m.Poll.Observe(0.002)
	})
}

func TestNewMetrics_WithRegisterer_RecordsValues(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.Enqueued.WithLabelValues("send_email").Inc()
	m.Enqueued.WithLabelValues("send_email").Inc()
	m.Processed.WithLabelValues("send_email", "done").Inc()
	m.InFlight.Inc()
	m.InFlight.Inc()
	m.InFlight.Dec()

	assert.Equal(t, float64(2), testutil.ToFloat64(m.Enqueued.WithLabelValues("send_email")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.Processed.WithLabelValues("send_email", "done")))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.InFlight))
}

func TestNewMetrics_WithRegisterer_RegistersAllFive(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	// Touch each metric so Gather sees at least one time series.
	m.Enqueued.WithLabelValues("_").Inc()
	m.Processed.WithLabelValues("_", "_").Inc()
	m.Duration.WithLabelValues("_").Observe(0.001)
	m.InFlight.Set(0)
	m.Poll.Observe(0.001)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	for _, want := range []string{
		"notasap_jobs_enqueued_total",
		"notasap_jobs_processed_total",
		"notasap_job_duration_seconds",
		"notasap_jobs_in_flight",
		"notasap_poll_duration_seconds",
	} {
		assert.True(t, names[want], "metric %q not registered", want)
	}
}

func TestNewMetrics_DoubleRegister_Panics(t *testing.T) {
	reg := prometheus.NewRegistry()
	_ = NewMetrics(reg)
	// Constructing a second Metrics on the same registry must panic via MustRegister.
	assert.Panics(t, func() { _ = NewMetrics(reg) })
}
