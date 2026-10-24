package prom_test

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/prom"
)

func TestNewErrors(t *testing.T) {
	_, err := prom.New(nil)
	require.Error(t, err)

	reg := prometheus.NewRegistry()
	_, err = prom.New(reg)
	require.NoError(t, err)
	_, err = prom.New(reg)
	require.Error(t, err)

	_, err = prom.New(prometheus.NewRegistry(), prom.WithJobDurationBuckets(nil))
	require.Error(t, err)
	_, err = prom.New(prometheus.NewRegistry(), prom.WithPollDurationBuckets([]float64{}))
	require.Error(t, err)
}

func TestNewFailureLeavesRegistryClean(t *testing.T) {
	reg := prometheus.NewRegistry()
	blocker := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notasap_janitor_deleted_total", Help: "x"}, []string{"status"})
	require.NoError(t, reg.Register(blocker))

	_, err := prom.New(reg)
	require.Error(t, err)

	mfs, err := reg.Gather()
	require.NoError(t, err)
	require.Empty(t, mfs)

	// Earlier collectors were unregistered, so their names are free again.
	again := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notasap_jobs_scheduled_total", Help: "Jobs scheduled."}, []string{"kind"})
	require.NoError(t, reg.Register(again))
}

func TestObserverMethods(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := prom.New(reg)
	require.NoError(t, err)

	m.JobScheduled("a")
	m.JobStarted("a")
	m.JobStarted("a")
	m.JobEnded("a")
	m.JobFinished("a", notasap.StatusDone, time.Second)
	m.JobRetried("a", notasap.RetryPanic, time.Second)
	m.JobStuck("a")
	m.JobRescued("a")
	m.Polled("a", time.Millisecond)
	m.JanitorDeleted(notasap.StatusDone, 5)

	require.EqualValues(t, 1, value(t, reg, "notasap_jobs_scheduled_total", "a"))
	require.EqualValues(t, 1, value(t, reg, "notasap_jobs_running", "a"))
	require.EqualValues(t, 1, value(t, reg, "notasap_jobs_finished_total", "a", "done"))
	require.EqualValues(t, 1, value(t, reg, "notasap_jobs_retried_total", "a", "panic"))
	require.EqualValues(t, 1, value(t, reg, "notasap_jobs_stuck_total", "a"))
	require.EqualValues(t, 1, value(t, reg, "notasap_jobs_rescued_total", "a"))
	require.EqualValues(t, 5, value(t, reg, "notasap_janitor_deleted_total", "done"))

	require.Equal(t, 1, histCount(t, reg, "notasap_job_duration_seconds", "outcome", "done"))
	require.Equal(t, 1, histCount(t, reg, "notasap_job_duration_seconds", "outcome", "retried"))
	require.Equal(t, 1, histCount(t, reg, "notasap_poll_duration_seconds", "kind", "a"))
}

func TestCustomBuckets(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := prom.New(reg, prom.WithJobDurationBuckets([]float64{1, 2}), prom.WithPollDurationBuckets([]float64{3}))
	require.NoError(t, err)
	m.JobFinished("a", notasap.StatusFailed, time.Second)
	m.Polled("a", time.Second)

	require.Equal(t, []float64{1, 2}, bounds(t, reg, "notasap_job_duration_seconds"))
	require.Equal(t, []float64{3}, bounds(t, reg, "notasap_poll_duration_seconds"))
}

func histCount(t *testing.T, reg *prometheus.Registry, name, label, value string) int {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, mt := range mf.GetMetric() {
			for _, l := range mt.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					return int(mt.GetHistogram().GetSampleCount())
				}
			}
		}
	}
	t.Fatalf("no series %s{%s=%q}", name, label, value)
	return 0
}

func bounds(t *testing.T, reg *prometheus.Registry, name string) []float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var out []float64
		for _, b := range mf.GetMetric()[0].GetHistogram().GetBucket() {
			out = append(out, b.GetUpperBound())
		}
		return out
	}
	t.Fatalf("no metric %s", name)
	return nil
}

// value returns the counter/gauge series whose label values equal vals in
// label-name order (labels sorted alphabetically, as Gather returns them).
func value(t *testing.T, reg *prometheus.Registry, name string, vals ...string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, mt := range mf.GetMetric() {
			ls := mt.GetLabel()
			if len(ls) != len(vals) {
				continue
			}
			for i, l := range ls {
				if l.GetValue() != vals[i] {
					continue series
				}
			}
			if mt.Counter != nil {
				return mt.GetCounter().GetValue()
			}
			return mt.GetGauge().GetValue()
		}
	}
	t.Fatalf("no series %s%v", name, vals)
	return 0
}

func TestInvalidBuckets(t *testing.T) {
	for name, b := range map[string][]float64{
		"decreasing": {2, 1},
		"equal":      {1, 1},
		"nan":        {math.NaN()},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := prom.New(prometheus.NewRegistry(), prom.WithJobDurationBuckets(b))
			require.Error(t, err)
			_, err = prom.New(prometheus.NewRegistry(), prom.WithPollDurationBuckets(b))
			require.Error(t, err)
		})
	}
}
