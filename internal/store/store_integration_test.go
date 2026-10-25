package store_test

import (
	"cmp"
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Run(m))
}

type StoreSuite struct {
	pgtest.Suite
	st *store.Store
}

func TestStoreSuite(t *testing.T) {
	suite.Run(t, new(StoreSuite))
}

func (s *StoreSuite) SetupSuite() {
	s.Suite.SetupSuite()
	s.st = store.New(s.Pool)
}

// pending inserts a due pending job of kind.
func (s *StoreSuite) pending(kind int16, runIn time.Duration) int64 {
	s.T().Helper()
	return s.InsertJob(pgtest.WithKind(kind), pgtest.WithRunIn(runIn))
}

// running inserts a running job of kind with the given attempts and a
// deadline deadlineIn from now.
func (s *StoreSuite) running(kind int16, attempt, maxAttempts int, deadlineIn time.Duration) int64 {
	s.T().Helper()
	return s.InsertJob(pgtest.WithKind(kind), pgtest.Running(deadlineIn), pgtest.WithAttempt(attempt, maxAttempts))
}

func (s *StoreSuite) TestClaim() {
	s.Run("order and limit", func() {
		// Without the claim index Postgres would otherwise return index order
		// even if the query lost its ORDER BY, hiding the bug.
		var indexDef string
		s.Require().NoError(s.Pool.QueryRow(context.Background(),
			`SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = 'notasap_jobs_claim_idx'`,
			s.Schema).Scan(&indexDef))
		s.Exec("DROP INDEX notasap_jobs_claim_idx")
		s.T().Cleanup(func() { s.Exec(indexDef) })
		// run_at order differs from id order: latest has the lowest id, the two
		// ties share one NOW() and so an exact run_at.
		latest := s.pending(1, -time.Second)
		ties := s.InsertJobs(2, pgtest.WithRunIn(-3*time.Second))
		tieLow, tieHigh := ties[0], ties[1]
		mid := s.pending(1, -2*time.Second)
		s.pending(2, -10*time.Second)
		s.pending(1, time.Hour)

		ctx := context.Background()
		// LIMIT 1 on the tie: the id tie-break picks the lower id.
		jobs, err := s.st.Claim(ctx, 1, 1, time.Minute)
		s.Require().NoError(err)
		s.Require().Len(jobs, 1)
		s.Equal(tieLow, jobs[0].ID)

		more, err := s.st.Claim(ctx, 1, 2, time.Minute)
		s.Require().NoError(err)
		s.Require().Len(more, 2)
		// RETURNING order is not guaranteed: compare sorted by id.
		slices.SortFunc(more, func(a, b store.ClaimedJob) int { return cmp.Compare(a.ID, b.ID) })
		s.Equal(tieHigh, more[0].ID)
		s.Equal(mid, more[1].ID)
		s.Equal(store.StatusPending, s.Job(latest).Status)

		now := s.DBNow()
		for _, j := range append(jobs, more...) {
			s.Equal(int16(1), j.Kind)
			s.Equal(1, j.Attempt)
			s.Equal(int64(1), j.Lease)
			s.Equal(3, j.MaxAttempts)
			s.JSONEq("{}", string(j.Payload))
			row := s.Job(j.ID)
			s.Equal(store.StatusRunning, row.Status)
			s.Require().NotNil(row.Deadline)
			s.WithinDuration(now.Add(time.Minute), *row.Deadline, 5*time.Second)
		}
	})

	s.Run("skips locked", func() {
		s.pending(1, -time.Second)
		ctx := context.Background()
		tx, err := s.Pool.Begin(ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(ctx) }()
		_, err = tx.Exec(ctx, "SELECT id FROM notasap_jobs FOR UPDATE")
		s.Require().NoError(err)

		jobs, err := s.st.Claim(ctx, 1, 10, time.Minute)
		s.Require().NoError(err)
		s.Empty(jobs)
	})
}

// claimOne inserts a due job of kind 1 and claims it.
func (s *StoreSuite) claimOne() store.ClaimedJob {
	s.T().Helper()
	s.pending(1, -time.Second)
	jobs, err := s.st.Claim(context.Background(), 1, 1, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(jobs, 1)
	return jobs[0]
}

func (s *StoreSuite) TestFinish() {
	s.Run("fenced by lease and status", func() {
		j := s.claimOne()
		ctx := context.Background()

		ok, err := s.st.Finish(ctx, store.Ref{ID: j.ID, Lease: j.Lease + 1}, store.StatusDone, "")
		s.Require().NoError(err)
		s.False(ok)
		row := s.Job(j.ID)
		s.Equal(store.StatusRunning, row.Status)
		s.NotNil(row.Deadline)

		ok, err = s.st.Finish(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, store.StatusDone, "")
		s.Require().NoError(err)
		s.True(ok)
		row = s.Job(j.ID)
		s.Equal(store.StatusDone, row.Status)
		s.Nil(row.Deadline)
		s.Nil(row.Error)

		ok, err = s.st.Finish(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, store.StatusFailed, "x")
		s.Require().NoError(err)
		s.False(ok)
	})

	s.Run("stores error", func() {
		j := s.claimOne()
		ok, err := s.st.Finish(context.Background(), store.Ref{ID: j.ID, Lease: j.Lease}, store.StatusFailed, "bad")
		s.Require().NoError(err)
		s.True(ok)
		row := s.Job(j.ID)
		s.Require().NotNil(row.Error)
		s.Equal("bad", *row.Error)
	})
}

func (s *StoreSuite) TestReschedule() {
	ctx := context.Background()
	s.Run("refund", func() {
		j := s.claimOne()
		ok, err := s.st.Reschedule(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, time.Minute, "again", true)
		s.Require().NoError(err)
		s.True(ok)
		row := s.Job(j.ID)
		s.Equal(0, row.Attempt)
		s.Equal(store.StatusPending, row.Status)
		s.Nil(row.Deadline)
		s.WithinDuration(s.DBNow().Add(time.Minute), row.RunAt, 5*time.Second)
		s.Require().NotNil(row.Error)
		s.Equal("again", *row.Error)
	})
	s.Run("no refund", func() {
		j := s.claimOne()
		ok, err := s.st.Reschedule(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, 0, "", false)
		s.Require().NoError(err)
		s.True(ok)
		row := s.Job(j.ID)
		s.Equal(1, row.Attempt)
		s.Equal(store.StatusPending, row.Status)
		s.Nil(row.Error)

		// Already rescheduled: the job is no longer running.
		ok, err = s.st.Reschedule(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, 0, "", false)
		s.Require().NoError(err)
		s.False(ok)
	})
	s.Run("stale lease", func() {
		j := s.claimOne()
		ok, err := s.st.Reschedule(ctx, store.Ref{ID: j.ID, Lease: j.Lease + 1}, 0, "", false)
		s.Require().NoError(err)
		s.False(ok)
		s.Equal(store.StatusRunning, s.Job(j.ID).Status)
	})
}

func (s *StoreSuite) TestCancelOutcomes() {
	ctx := context.Background()
	tests := []struct {
		name   string
		status int16
		want   store.CancelOutcome
	}{
		{"pending", store.StatusPending, store.CancelOK},
		{"running", store.StatusRunning, store.CancelRunning},
		{"done", store.StatusDone, store.CancelFinished},
		{"failed", store.StatusFailed, store.CancelFinished},
		{"cancelled", store.StatusCancelled, store.CancelFinished},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			id := s.InsertJob(pgtest.WithStatus(tt.status))
			got, err := s.st.Cancel(ctx, id)
			s.Require().NoError(err)
			s.Equal(tt.want, got)
		})
	}
	s.Run("not found", func() {
		got, err := s.st.Cancel(ctx, -1)
		s.Require().NoError(err)
		s.Equal(store.CancelNotFound, got)
	})
}

func (s *StoreSuite) TestCheckSchemaMissing() {
	s.Require().NoError(s.st.CheckSchema(context.Background()))

	schema := "nt_empty_" + s.Schema
	s.Exec("CREATE SCHEMA " + schema)
	s.T().Cleanup(func() { s.Exec("DROP SCHEMA " + schema + " CASCADE") })
	pool := pgtest.NewPool(s.T(), schema)

	err := store.New(pool).CheckSchema(context.Background())
	s.Require().Error(err)
	s.Contains(err.Error(), "apply migrations")
}

func (s *StoreSuite) TestRescue() {
	s.Run("retries or fails expired jobs", func() {
		retry := s.running(1, 1, 3, -time.Minute)
		fail := s.running(7, 3, 3, -2*time.Minute)
		live := s.running(1, 1, 3, time.Hour)
		pending := s.pending(1, -time.Second)

		var seen []store.StuckJob
		n, err := s.st.Rescue(context.Background(), 10, func(j store.StuckJob) store.RescueDecision {
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

		row := s.Job(retry)
		s.Equal(store.StatusPending, row.Status)
		s.Equal(1, row.Attempt)
		s.Equal(int64(1), row.Lease)
		s.Nil(row.Deadline)
		s.WithinDuration(s.DBNow().Add(time.Hour), row.RunAt, 5*time.Second)
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)

		row = s.Job(fail)
		s.Equal(store.StatusFailed, row.Status)
		s.Nil(row.Deadline)
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)

		s.Equal(store.StatusRunning, s.Job(live).Status)
		s.Equal(store.StatusPending, s.Job(pending).Status)
	})

	s.Run("limit and skip locked", func() {
		locked := s.running(1, 1, 3, -3*time.Minute)
		first := s.running(1, 1, 3, -2*time.Minute)
		s.running(1, 1, 3, -time.Minute)
		ctx := context.Background()
		tx, err := s.Pool.Begin(ctx)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback(ctx) }()
		_, err = tx.Exec(ctx, "SELECT id FROM notasap_jobs WHERE id = $1 FOR UPDATE", locked)
		s.Require().NoError(err)

		var seen []int64
		n, err := s.st.Rescue(ctx, 1, func(j store.StuckJob) store.RescueDecision {
			seen = append(seen, j.ID)
			return store.RescueDecision{}
		})
		s.Require().NoError(err)
		s.Equal(1, n)
		s.Equal([]int64{first}, seen)
		s.Equal(store.StatusPending, s.Job(first).Status)
	})

	s.Run("nothing expired", func() {
		s.running(1, 1, 3, time.Hour)
		n, err := s.st.Rescue(context.Background(), 10, func(store.StuckJob) store.RescueDecision {
			s.Fail("decide called")
			return store.RescueDecision{}
		})
		s.Require().NoError(err)
		s.Zero(n)
	})
}

func (s *StoreSuite) TestDeleteFinished() {
	oldest := s.InsertJob(pgtest.Finished(store.StatusDone, 5*time.Hour))
	older := s.InsertJob(pgtest.Finished(store.StatusDone, 4*time.Hour))
	old := s.InsertJob(pgtest.Finished(store.StatusDone, 3*time.Hour))
	fresh := s.InsertJob(pgtest.Finished(store.StatusDone, time.Minute))
	failed := s.InsertJob(pgtest.Finished(store.StatusFailed, 5*time.Hour))
	pending := s.InsertJob(pgtest.WithUpdatedAgo(5 * time.Hour))

	ctx := context.Background()
	n, err := s.st.DeleteFinished(ctx, store.StatusDone, time.Hour, 2)
	s.Require().NoError(err)
	s.Equal(int64(2), n)
	s.False(s.Exists(oldest))
	s.False(s.Exists(older))
	s.True(s.Exists(old))

	n, err = s.st.DeleteFinished(ctx, store.StatusDone, time.Hour, 2)
	s.Require().NoError(err)
	s.Equal(int64(1), n)
	s.False(s.Exists(old))

	n, err = s.st.DeleteFinished(ctx, store.StatusDone, time.Hour, 2)
	s.Require().NoError(err)
	s.Zero(n)
	s.True(s.Exists(fresh))
	s.True(s.Exists(failed))
	s.True(s.Exists(pending))
}

func (s *StoreSuite) TestInsertOutcomesInParamsOrder() {
	ctx := context.Background()
	key := func(k string) *string { return &k }
	seed, err := s.st.Insert(ctx, s.Pool, []store.InsertParams{
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("dup")},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("done")},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("run")},
	})
	s.Require().NoError(err)
	s.SetJob(seed[1].ID, pgtest.WithStatus(store.StatusDone))
	s.SetJob(seed[2].ID, pgtest.WithStatus(store.StatusRunning))

	res, err := s.st.Insert(ctx, s.Pool, []store.InsertParams{
		{Kind: 2, Payload: []byte("{}"), MaxAttempts: 3, Key: key("dup")},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("run"), Replace: true},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("done"), Replace: true},
		{Kind: 1, Payload: []byte("{}"), MaxAttempts: 3, Key: key("dup")},
	})
	s.Require().NoError(err)
	s.Require().Len(res, 5)
	s.Equal(store.Inserted, res[0].Outcome)
	s.Equal(store.InsertResult{ID: seed[2].ID, Outcome: store.Running}, res[1])
	s.Equal(store.Inserted, res[2].Outcome)
	s.Equal(store.InsertResult{ID: seed[1].ID, Outcome: store.Replaced}, res[3])
	s.Equal(store.InsertResult{ID: seed[0].ID, Outcome: store.Duplicate}, res[4])
	// The two inserted rows are new: non-zero, distinct, not a seed id.
	for _, r := range []store.InsertResult{res[0], res[2]} {
		s.NotZero(r.ID)
		for _, sd := range seed {
			s.NotEqual(sd.ID, r.ID)
		}
	}
	s.NotEqual(res[0].ID, res[2].ID)
	s.Equal(store.StatusPending, s.Job(seed[1].ID).Status)
	s.Equal(store.StatusRunning, s.Job(seed[2].ID).Status)
}
