package store_test

import (
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type rescueSuite struct {
	testSuite
}

func TestRescue(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(rescueSuite))
}

func (s *rescueSuite) TestStore_Rescue() {
	s.Run("retries or fails expired jobs", func() {
		retry := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(1)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-time.Minute),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		}).ID
		fail := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(7)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-2 * time.Minute),
			Attempt:     lo.ToPtr(3),
			MaxAttempts: lo.ToPtr(3),
		}).ID
		live := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(1)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(time.Hour),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		}).ID
		pending := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:  lo.ToPtr(int16(1)),
			RunIn: lo.ToPtr(-time.Second),
		}).ID

		var seen []store.StuckJob
		n, err := s.st.Rescue(s.ctx, 10, func(j store.StuckJob) store.RescueDecision {
			seen = append(seen, j)
			if j.ID == fail {
				return store.RescueDecision{Fail: true}
			}
			return store.RescueDecision{Delay: time.Hour}
		})
		s.Require().NoError(err)
		s.Equal(2, n)
		s.Equal([]store.StuckJob{
			{ID: fail, Kind: 7, Attempt: 3, MaxAttempts: 3},
			{ID: retry, Kind: 1, Attempt: 1, MaxAttempts: 3},
		}, seen)

		row := s.GetJob(retry)
		s.Equal(store.StatusPending, row.Status)
		s.Equal(1, row.Attempt)
		s.Equal(int64(1), row.Lease)
		s.Nil(row.Deadline)
		s.WithinDuration(s.DBNow().Add(time.Hour), row.RunAt, 5*time.Second)
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)

		row = s.GetJob(fail)
		s.Equal(store.StatusFailed, row.Status)
		s.Nil(row.Deadline)
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)

		s.Equal(store.StatusRunning, s.GetJob(live).Status)
		s.Equal(store.StatusPending, s.GetJob(pending).Status)
	})

	s.Run("limit and skip locked", func() {
		locked := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(1)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-3 * time.Minute),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		}).ID
		first := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(1)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-2 * time.Minute),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		}).ID
		s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(1)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-time.Minute),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		})
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(s.ctx) }()
		_, err = tx.Exec(s.ctx, "SELECT id FROM notasap_jobs WHERE id = $1 FOR UPDATE", locked)
		s.Require().NoError(err)

		var seen []int64
		n, err := s.st.Rescue(s.ctx, 1, func(j store.StuckJob) store.RescueDecision {
			seen = append(seen, j.ID)
			return store.RescueDecision{}
		})
		s.Require().NoError(err)
		s.Equal(1, n)
		s.Equal([]int64{first}, seen)
		s.Equal(store.StatusPending, s.GetJob(first).Status)
	})

	s.Run("nothing expired", func() {
		s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(1)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(time.Hour),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		})
		n, err := s.st.Rescue(s.ctx, 10, func(store.StuckJob) store.RescueDecision {
			s.Fail("decide called")
			return store.RescueDecision{}
		})
		s.Require().NoError(err)
		s.Zero(n)
	})
}
