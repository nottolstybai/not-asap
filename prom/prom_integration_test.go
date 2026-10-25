package prom_test

import (
	"context"
	"log/slog"
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

	c, err := notasap.New(s.Pool, notasap.WithObserver(m), notasap.WithLogger(slog.New(slog.DiscardHandler)))
	s.Require().NoError(err)
	s.Require().NoError(notasap.Register(c,
		func(context.Context, *notasap.Job[payloadA]) error { return nil },
		notasap.KindConfig{Name: "a", PollInterval: 10 * time.Millisecond}))

	ctx := context.Background()
	s.Require().NoError(c.Start(ctx))
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.NoError(c.Stop(ctx))
	}
	s.T().Cleanup(stop)

	_, err = c.Schedule(ctx, payloadA{})
	s.Require().NoError(err)

	s.WaitFor(func() bool {
		v, _ := lookup(reg, "notasap_jobs_finished_total", "a", "done")
		return v == 1
	}, "job not finished")
	// Stop drains the handler including its observer calls, so the
	// histogram observed after the counter is complete too.
	stop()

	s.EqualValues(1, value(s.T(), reg, "notasap_jobs_scheduled_total", "a"))
	s.Equal(1, histCount(s.T(), reg, "notasap_job_duration_seconds", "outcome", "done"))
}
