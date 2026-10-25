package notasap

import (
	"context"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type janitorSuite struct {
	testSuite
}

func TestJanitor(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(janitorSuite))
}

// TestClient_clean drives an unstarted client: no background pass can race
// with clean.
func (s *janitorSuite) TestClient_clean() {
	s.Run("default retention", func() {
		doneOld := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		}).ID
		cancelledOld := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusCancelled),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		}).ID
		doneNew := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(time.Hour),
		}).ID
		failedOld := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusFailed),
			UpdatedAgo: lo.ToPtr(30 * 24 * time.Hour),
		}).ID

		s.newClient(nil).clean(s.ctx)
		s.False(s.Exists(doneOld))
		s.False(s.Exists(cancelledOld))
		s.True(s.Exists(doneNew))
		s.True(s.Exists(failedOld))
	})

	s.Run("custom retention merged", func() {
		doneOld := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(2 * time.Minute),
		}).ID
		cancelledOld := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusCancelled),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		}).ID
		cancelledNew := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusCancelled),
			UpdatedAgo: lo.ToPtr(time.Hour),
		}).ID

		s.newClient(nil, WithJanitor(JanitorConfig{
			Retention: map[Status]time.Duration{StatusDone: time.Minute},
		})).clean(s.ctx)
		s.False(s.Exists(doneOld))
		s.False(s.Exists(cancelledOld))
		s.True(s.Exists(cancelledNew))
	})

	s.Run("batches", func() {
		s.Fixture.Jobs(s.ctx, 25, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		})
		c := s.newClient(nil, WithJanitor(JanitorConfig{BatchSize: 10}))

		c.clean(s.ctx)
		s.Zero(s.CountJobs())
		s.Equal([]int64{10, 10, 5}, c.obs.Batches())
	})
}

func (s *janitorSuite) TestClient_janitorLoop() {
	// Smoke test only: Stop cancels the loops, so a janitor that ignored
	// Disabled could still miss its first pass. TestJanitorLoopStartsUnlessDisabled
	// (unit) is the real check.
	s.Run("disabled smoke", func() {
		s.Fixture.Jobs(s.ctx, 2, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		})

		c := s.startClient(nil, WithJanitor(JanitorConfig{Disabled: true}))
		s.stop(c) // no maintenance loop runs after Stop
		s.Equal(2, s.CountJobs())
		s.Empty(c.obs.Batches())
	})

	s.Run("stop interrupts pass", func() {
		s.Fixture.Jobs(s.ctx, 5000, pgtest.JobOpt{
			Status:     lo.ToPtr(store.StatusDone),
			UpdatedAgo: lo.ToPtr(25 * time.Hour),
		})

		c := s.startClient(nil, WithJanitor(JanitorConfig{BatchSize: 1}))
		s.WaitFor(func() bool { return s.CountJobs() < 5000 }, "janitor did not start deleting")

		// The bound only rules out finishing 5000 serial deletes; the unit test
		// TestStopCancelsJanitorPass proves the interruption without a clock.
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		defer cancel()
		start := time.Now()
		s.NoError(c.Stop(ctx))
		s.Less(time.Since(start), 5*time.Second)
		s.Positive(s.CountJobs())
	})
}
