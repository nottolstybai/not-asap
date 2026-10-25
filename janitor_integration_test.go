package notasap_test

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type JanitorSuite struct {
	clientSuite
}

func TestJanitorSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(JanitorSuite))
}

// TestCleanNow drives an unstarted client: no background pass can race with
// CleanNow.
func (s *JanitorSuite) TestCleanNow() {
	s.Run("default retention", func() {
		doneOld := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		}).ID
		cancelledOld := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusCancelled),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		}).ID
		doneNew := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(time.Hour),
		}).ID
		failedOld := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusFailed),
			UpdatedAgo: lo.ToPtr(30 * 24 * time.Hour),
		}).ID

		s.newClient(nil).CleanNow(context.Background())
		s.False(s.Exists(doneOld))
		s.False(s.Exists(cancelledOld))
		s.True(s.Exists(doneNew))
		s.True(s.Exists(failedOld))
	})

	s.Run("custom retention merged", func() {
		doneOld := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(2 * time.Minute),
		}).ID
		cancelledOld := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusCancelled),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		}).ID
		cancelledNew := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusCancelled),
			UpdatedAgo: lo.ToPtr(time.Hour),
		}).ID

		s.newClient(nil, notasap.WithJanitor(notasap.JanitorConfig{
			Retention: map[notasap.Status]time.Duration{notasap.StatusDone: time.Minute},
		})).CleanNow(context.Background())
		s.False(s.Exists(doneOld))
		s.False(s.Exists(cancelledOld))
		s.True(s.Exists(cancelledNew))
	})

	s.Run("batches", func() {
		s.Fixture.Jobs(context.Background(), 25, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		})
		c := s.newClient(nil, notasap.WithJanitor(notasap.JanitorConfig{BatchSize: 10}))

		c.CleanNow(context.Background())
		s.Zero(s.CountJobs())
		s.Equal([]int64{10, 10, 5}, c.obs.Batches())
	})
}

func (s *JanitorSuite) TestLoop() {
	// Smoke test only: Stop cancels the loops, so a janitor that ignored
	// Disabled could still miss its first pass. TestJanitorLoopStartsUnlessDisabled
	// (unit) is the real check.
	s.Run("disabled smoke", func() {
		s.Fixture.Jobs(context.Background(), 2, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		})

		c := s.startClient(nil, notasap.WithJanitor(notasap.JanitorConfig{Disabled: true}))
		s.stop(c) // no maintenance loop runs after Stop
		s.Equal(2, s.CountJobs())
		s.Empty(c.obs.Batches())
	})

	s.Run("stop interrupts pass", func() {
		s.Fixture.Jobs(context.Background(), 5000, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		})

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
