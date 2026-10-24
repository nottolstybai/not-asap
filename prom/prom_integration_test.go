package prom_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/prom"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Run(m))
}

type payloadA struct{}

func (payloadA) Kind() notasap.Kind { return 1 }

type PromSuite struct {
	pgtest.Suite
}

func TestPromSuite(t *testing.T) {
	suite.Run(t, new(PromSuite))
}

func (s *PromSuite) TestMetricsAfterProcessing() {
	reg := prometheus.NewRegistry()
	m, err := prom.New(reg)
	s.Require().NoError(err)

	c, err := notasap.New(s.Pool, notasap.WithObserver(m))
	s.Require().NoError(err)
	s.Require().NoError(notasap.Register(c,
		func(context.Context, *notasap.Job[payloadA]) error { return nil },
		notasap.KindConfig{Name: "a", PollInterval: 10 * time.Millisecond}))

	ctx := context.Background()
	s.Require().NoError(c.Start(ctx))
	s.T().Cleanup(func() { _ = c.Stop(context.Background()) })

	_, err = c.Schedule(ctx, payloadA{})
	s.Require().NoError(err)

	s.Require().Eventually(func() bool {
		return seriesValue(reg, "notasap_jobs_finished_total", "a", "done") == 1
	}, 10*time.Second, 20*time.Millisecond)

	s.EqualValues(1, value(s.T(), reg, "notasap_jobs_scheduled_total", "a"))
	s.Equal(1, histCount(s.T(), reg, "notasap_job_duration_seconds", "outcome", "done"))
}

// seriesValue is a non-fatal variant of value for use inside Eventually.
func seriesValue(reg *prometheus.Registry, name string, vals ...string) float64 {
	mfs, _ := reg.Gather()
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
			return mt.GetCounter().GetValue()
		}
	}
	return 0
}
