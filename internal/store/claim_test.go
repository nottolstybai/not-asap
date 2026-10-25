package store_test

import (
	"cmp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type claimSuite struct {
	testSuite
}

func TestClaim(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(claimSuite))
}

func (s *claimSuite) TestStore_Claim() {
	s.Run("order and limit", func() {
		// Without the claim index Postgres would otherwise return index order
		// even if the query lost its ORDER BY, hiding the bug.
		var indexDef string
		s.Require().NoError(s.Pool.QueryRow(s.ctx,
			`SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = 'notasap_jobs_claim_idx'`,
			s.Schema).Scan(&indexDef))
		s.Exec("DROP INDEX notasap_jobs_claim_idx")
		s.ExecOnCleanup(indexDef)
		// run_at order differs from id order: latest has the lowest id, the two
		// ties share one NOW() and so an exact run_at.
		latest := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:  new(int16(1)),
			RunIn: new(-time.Second),
		}).ID
		ties := s.Fixture.Jobs(s.ctx, 2, pgtest.JobOpt{RunIn: new(-3 * time.Second)})
		slices.SortFunc(ties, func(a, b pgtest.JobRow) int { return cmp.Compare(a.ID, b.ID) }) // RETURNING order is not guaranteed
		tieLow, tieHigh := ties[0].ID, ties[1].ID
		mid := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:  new(int16(1)),
			RunIn: new(-2 * time.Second),
		}).ID
		s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:  new(int16(2)),
			RunIn: new(-10 * time.Second),
		})
		s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:  new(int16(1)),
			RunIn: new(time.Hour),
		})

		// LIMIT 1 on the tie: the id tie-break picks the lower id.
		jobs, err := s.st.Claim(s.ctx, 1, 1, time.Minute)
		s.Require().NoError(err)
		s.Require().Len(jobs, 1)
		s.Equal(tieLow, jobs[0].ID)

		more, err := s.st.Claim(s.ctx, 1, 2, time.Minute)
		s.Require().NoError(err)
		s.Require().Len(more, 2)
		// RETURNING order is not guaranteed: compare sorted by id.
		slices.SortFunc(more, func(a, b store.ClaimedJob) int { return cmp.Compare(a.ID, b.ID) })
		s.Equal(tieHigh, more[0].ID)
		s.Equal(mid, more[1].ID)
		s.Equal(store.StatusPending, s.GetJob(latest).Status)

		now := s.DBNow()
		for _, j := range append(jobs, more...) {
			s.Equal(int16(1), j.Kind)
			s.Equal(1, j.Attempt)
			s.Equal(int64(1), j.Lease)
			s.Equal(3, j.MaxAttempts)
			s.JSONEq("{}", string(j.Payload))
			row := s.GetJob(j.ID)
			s.Equal(store.StatusRunning, row.Status)
			s.Require().NotNil(row.Deadline)
			s.WithinDuration(now.Add(time.Minute), *row.Deadline, 5*time.Second)
		}
	})

	s.Run("skips locked", func() {
		s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:  new(int16(1)),
			RunIn: new(-time.Second),
		})
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(s.ctx) }()
		_, err = tx.Exec(s.ctx, "SELECT id FROM notasap_jobs FOR UPDATE")
		s.Require().NoError(err)

		jobs, err := s.st.Claim(s.ctx, 1, 10, time.Minute)
		s.Require().NoError(err)
		s.Empty(jobs)
	})
}

// claimOne inserts a due job of kind 1 and claims it.
func (s *claimSuite) claimOne() store.ClaimedJob {
	s.T().Helper()
	s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Kind:  new(int16(1)),
		RunIn: new(-time.Second),
	})
	jobs, err := s.st.Claim(s.ctx, 1, 1, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(jobs, 1)
	return jobs[0]
}

func (s *claimSuite) TestStore_Complete() {
	s.Run("fenced by lease and status", func() {
		j := s.claimOne()
		ref := store.Ref{
			ID:    j.ID,
			Lease: j.Lease,
		}
		stale := store.Ref{
			ID:    j.ID,
			Lease: j.Lease + 1,
		}

		written, err := s.st.Complete(s.ctx, []store.Completion{{
			Ref:    stale,
			Status: store.StatusDone,
		}})
		s.Require().NoError(err)
		s.Empty(written)
		row := s.GetJob(j.ID)
		s.Equal(store.StatusRunning, row.Status)
		s.NotNil(row.Deadline)

		written, err = s.st.Complete(s.ctx, []store.Completion{{
			Ref:    ref,
			Status: store.StatusDone,
		}})
		s.Require().NoError(err)
		s.Equal([]store.Ref{ref}, written)
		row = s.GetJob(j.ID)
		s.Equal(store.StatusDone, row.Status)
		s.Nil(row.Deadline)
		s.Nil(row.Error)

		// Already finished: the job is no longer running.
		written, err = s.st.Complete(s.ctx, []store.Completion{{
			Ref:    ref,
			Status: store.StatusFailed,
			Error:  "x",
		}})
		s.Require().NoError(err)
		s.Empty(written)
		s.Equal(store.StatusDone, s.GetJob(j.ID).Status)
	})

	s.Run("mixed batch", func() {
		failed, retried, refunded, plain, lost := s.claimOne(), s.claimOne(), s.claimOne(), s.claimOne(), s.claimOne()
		ref := func(j store.ClaimedJob) store.Ref {
			return store.Ref{
				ID:    j.ID,
				Lease: j.Lease,
			}
		}
		runAt := s.GetJob(failed.ID).RunAt

		// Input order is not id order.
		written, err := s.st.Complete(s.ctx, []store.Completion{
			{
				Ref:    ref(plain),
				Status: store.StatusPending,
			},
			{
				Ref: store.Ref{
					ID:    lost.ID,
					Lease: lost.Lease + 1,
				},
				Status: store.StatusDone,
			},
			{
				Ref:    ref(failed),
				Status: store.StatusFailed,
				Error:  "bad",
			},
			{
				Ref:    ref(refunded),
				Status: store.StatusPending,
				Delay:  time.Minute,
				Error:  "again",
				Refund: true,
			},
			{
				Ref:    ref(retried),
				Status: store.StatusPending,
				Delay:  time.Hour,
				Error:  "retry",
			},
		})
		s.Require().NoError(err)
		s.ElementsMatch([]store.Ref{ref(failed), ref(retried), ref(refunded), ref(plain)}, written)

		now := s.DBNow()
		row := s.GetJob(failed.ID)
		s.Equal(store.StatusFailed, row.Status)
		s.Equal(1, row.Attempt)
		s.Equal(runAt, row.RunAt)
		s.Nil(row.Deadline)
		s.Require().NotNil(row.Error)
		s.Equal("bad", *row.Error)

		row = s.GetJob(retried.ID)
		s.Equal(store.StatusPending, row.Status)
		s.Equal(1, row.Attempt)
		s.Nil(row.Deadline)
		s.WithinDuration(now.Add(time.Hour), row.RunAt, 5*time.Second)
		s.Require().NotNil(row.Error)
		s.Equal("retry", *row.Error)

		row = s.GetJob(refunded.ID)
		s.Equal(store.StatusPending, row.Status)
		s.Equal(0, row.Attempt)
		s.WithinDuration(now.Add(time.Minute), row.RunAt, 5*time.Second)
		s.Require().NotNil(row.Error)
		s.Equal("again", *row.Error)

		row = s.GetJob(plain.ID)
		s.Equal(store.StatusPending, row.Status)
		s.Equal(1, row.Attempt)
		s.WithinDuration(now, row.RunAt, 5*time.Second)
		s.Nil(row.Error)

		row = s.GetJob(lost.ID)
		s.Equal(store.StatusRunning, row.Status)
		s.NotNil(row.Deadline)
	})

	s.Run("same id, two leases", func() {
		j := s.claimOne()
		ref := store.Ref{
			ID:    j.ID,
			Lease: j.Lease,
		}

		written, err := s.st.Complete(s.ctx, []store.Completion{
			{
				Ref: store.Ref{
					ID:    j.ID,
					Lease: j.Lease - 1,
				},
				Status: store.StatusFailed,
				Error:  "stale",
			},
			{
				Ref:    ref,
				Status: store.StatusDone,
			},
		})
		s.Require().NoError(err)
		s.Equal([]store.Ref{ref}, written)
		row := s.GetJob(j.ID)
		s.Equal(store.StatusDone, row.Status)
		s.Nil(row.Error)
	})

	s.Run("waits for locked", func() {
		j := s.claimOne()
		ref := store.Ref{
			ID:    j.ID,
			Lease: j.Lease,
		}
		tx, err := s.Pool.Begin(s.ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(s.ctx) }()
		_, err = tx.Exec(s.ctx, "SELECT id FROM notasap_jobs WHERE id = $1 FOR UPDATE", j.ID)
		s.Require().NoError(err)

		var (
			written     []store.Ref
			completeErr error
			wg          sync.WaitGroup
		)
		wg.Go(func() {
			written, completeErr = s.st.Complete(s.ctx, []store.Completion{{
				Ref:    ref,
				Status: store.StatusDone,
			}})
		})
		s.WaitBlockedBy(s.BackendPID(tx))
		s.Equal(store.StatusRunning, s.GetJob(j.ID).Status)

		s.Require().NoError(tx.Commit(s.ctx))
		wg.Wait()
		s.Require().NoError(completeErr)
		s.Equal([]store.Ref{ref}, written)
		s.Equal(store.StatusDone, s.GetJob(j.ID).Status)
	})

	s.Run("empty", func() {
		written, err := s.st.Complete(s.ctx, nil)
		s.Require().NoError(err)
		s.Empty(written)
	})
}
