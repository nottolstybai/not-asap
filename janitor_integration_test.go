package notasap_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type JanitorSuite struct {
	clientSuite
}

func TestJanitorSuite(t *testing.T) {
	suite.Run(t, new(JanitorSuite))
}

// TestCleanNow drives an unstarted client: no background pass can race with
// CleanNow.
func (s *JanitorSuite) TestCleanNow() {
	s.Run("default retention", func() {
		doneOld := s.InsertJob(pgtest.Finished(store.StatusDone, 25*time.Hour))
		cancelledOld := s.InsertJob(pgtest.Finished(store.StatusCancelled, 25*time.Hour))
		doneNew := s.InsertJob(pgtest.Finished(store.StatusDone, time.Hour))
		failedOld := s.InsertJob(pgtest.Finished(store.StatusFailed, 30*24*time.Hour))

		s.newClient(nil).CleanNow(context.Background())
		s.False(s.Exists(doneOld))
		s.False(s.Exists(cancelledOld))
		s.True(s.Exists(doneNew))
		s.True(s.Exists(failedOld))
	})

	s.Run("custom retention merged", func() {
		doneOld := s.InsertJob(pgtest.Finished(store.StatusDone, 2*time.Minute))
		cancelledOld := s.InsertJob(pgtest.Finished(store.StatusCancelled, 25*time.Hour))
		cancelledNew := s.InsertJob(pgtest.Finished(store.StatusCancelled, time.Hour))

		s.newClient(nil, notasap.WithJanitor(notasap.JanitorConfig{
			Retention: map[notasap.Status]time.Duration{notasap.StatusDone: time.Minute},
		})).CleanNow(context.Background())
		s.False(s.Exists(doneOld))
		s.False(s.Exists(cancelledOld))
		s.True(s.Exists(cancelledNew))
	})

	s.Run("batches", func() {
		s.InsertJobs(25, pgtest.Finished(store.StatusDone, 25*time.Hour))
		c := s.newClient(nil, notasap.WithJanitor(notasap.JanitorConfig{BatchSize: 10}))

		c.CleanNow(context.Background())
		s.Zero(s.CountJobs())
		s.Equal([]int64{10, 10, 5}, c.obs.Batches())
	})
}

func (s *JanitorSuite) TestLoop() {
	s.Run("disabled", func() {
		s.InsertJobs(2, pgtest.Finished(store.StatusDone, 25*time.Hour))

		c := s.startClient(nil, notasap.WithJanitor(notasap.JanitorConfig{Disabled: true}))
		s.stop(c) // no maintenance loop runs after Stop
		s.Equal(2, s.CountJobs())
		s.Empty(c.obs.Batches())
	})

	s.Run("stop interrupts pass", func() {
		s.InsertJobs(5000, pgtest.Finished(store.StatusDone, 25*time.Hour))

		c := s.startClient(nil, notasap.WithJanitor(notasap.JanitorConfig{BatchSize: 1}))
		s.WaitFor(func() bool { return s.CountJobs() < 5000 }, "janitor did not start deleting")

		// The bound only rules out finishing 5000 serial deletes; the unit test
		// TestStopCancelsJanitorPass proves the interruption without a clock.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		start := time.Now()
		s.Require().NoError(c.Stop(ctx))
		s.Less(time.Since(start), 5*time.Second)
		s.Positive(s.CountJobs())
	})
}
