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

// insertJob inserts a row with the given kind, status and run_at offset from now.
func (s *StoreSuite) insertJob(kind int16, status int16, runIn time.Duration) int64 {
	s.T().Helper()
	var id int64
	err := s.Pool.QueryRow(context.Background(), `
INSERT INTO notasap_jobs (kind, status, payload, max_attempts, run_at)
VALUES ($1, $2, '{}', 3, NOW() + make_interval(secs => $3)) RETURNING id`,
		kind, status, runIn.Seconds()).Scan(&id)
	s.Require().NoError(err)
	return id
}

func (s *StoreSuite) TestClaimOrderAndLimit() {
	// Without the claim index Postgres would otherwise return index order
	// even if the query lost its ORDER BY, hiding the bug.
	s.Exec("DROP INDEX notasap_jobs_claim_idx")
	s.T().Cleanup(func() {
		s.Exec("CREATE INDEX notasap_jobs_claim_idx ON notasap_jobs (kind, run_at, id) WHERE status = 1")
	})
	// run_at order differs from id order: id1 is the latest, id2 and id3 tie.
	latest := s.insertJob(1, 1, -time.Second)
	tieLow := s.insertJob(1, 1, -3*time.Second)
	tieHigh := s.insertJob(1, 1, -4*time.Second)
	s.Exec("UPDATE notasap_jobs SET run_at = (SELECT run_at FROM notasap_jobs WHERE id = $1) WHERE id = $2", tieLow, tieHigh)
	s.insertJob(1, 1, -2*time.Second)
	s.insertJob(2, 1, -10*time.Second)
	s.insertJob(1, 1, time.Hour)

	jobs, err := s.st.Claim(context.Background(), 1, 2, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(jobs, 2)
	// RETURNING order is not guaranteed: compare sorted by id.
	slices.SortFunc(jobs, func(a, b store.ClaimedJob) int { return cmp.Compare(a.ID, b.ID) })
	s.Equal(tieLow, jobs[0].ID)
	s.Equal(tieHigh, jobs[1].ID)
	s.NotEqual(latest, jobs[0].ID)
	for _, j := range jobs {
		s.Equal(int16(1), j.Kind)
		s.Equal(1, j.Attempt)
		s.Equal(int64(1), j.Lease)
		s.Equal(3, j.MaxAttempts)
		s.JSONEq("{}", string(j.Payload))
		row := s.Job(j.ID)
		s.Equal(int16(2), row.Status)
		s.Require().NotNil(row.Deadline)
		s.WithinDuration(time.Now().Add(time.Minute), *row.Deadline, 5*time.Second)
	}
}

func (s *StoreSuite) TestClaimSkipLocked() {
	s.insertJob(1, 1, -time.Second)
	ctx := context.Background()
	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, "SELECT id FROM notasap_jobs FOR UPDATE")
	s.Require().NoError(err)

	jobs, err := s.st.Claim(ctx, 1, 10, time.Minute)
	s.Require().NoError(err)
	s.Empty(jobs)
}

func (s *StoreSuite) claimOne() store.ClaimedJob {
	s.T().Helper()
	s.insertJob(1, 1, -time.Second)
	jobs, err := s.st.Claim(context.Background(), 1, 1, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(jobs, 1)
	return jobs[0]
}

func (s *StoreSuite) TestFinishFencing() {
	j := s.claimOne()
	ctx := context.Background()

	ok, err := s.st.Finish(ctx, store.Ref{ID: j.ID, Lease: j.Lease + 1}, 3, "")
	s.Require().NoError(err)
	s.False(ok)
	row := s.Job(j.ID)
	s.Equal(int16(2), row.Status)
	s.NotNil(row.Deadline)

	ok, err = s.st.Finish(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, 3, "")
	s.Require().NoError(err)
	s.True(ok)
	row = s.Job(j.ID)
	s.Equal(int16(3), row.Status)
	s.Nil(row.Deadline)
	s.Nil(row.Error)

	ok, err = s.st.Finish(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, 4, "x")
	s.Require().NoError(err)
	s.False(ok)
}

func (s *StoreSuite) TestFinishStoresError() {
	j := s.claimOne()
	ok, err := s.st.Finish(context.Background(), store.Ref{ID: j.ID, Lease: j.Lease}, 4, "bad")
	s.Require().NoError(err)
	s.True(ok)
	row := s.Job(j.ID)
	s.Require().NotNil(row.Error)
	s.Equal("bad", *row.Error)
}

func (s *StoreSuite) TestRescheduleRefund() {
	ctx := context.Background()

	j := s.claimOne()
	ok, err := s.st.Reschedule(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, time.Minute, "again", true)
	s.Require().NoError(err)
	s.True(ok)
	row := s.Job(j.ID)
	s.Equal(0, row.Attempt)
	s.Equal(int16(1), row.Status)
	s.Nil(row.Deadline)
	s.WithinDuration(time.Now().Add(time.Minute), row.RunAt, 5*time.Second)
	s.Require().NotNil(row.Error)
	s.Equal("again", *row.Error)

	s.Exec("TRUNCATE notasap_jobs RESTART IDENTITY")
	j = s.claimOne()
	ok, err = s.st.Reschedule(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, 0, "", false)
	s.Require().NoError(err)
	s.True(ok)
	row = s.Job(j.ID)
	s.Equal(1, row.Attempt)
	s.Equal(int16(1), row.Status)
	s.Nil(row.Error)

	ok, err = s.st.Reschedule(ctx, store.Ref{ID: j.ID, Lease: j.Lease}, 0, "", false)
	s.Require().NoError(err)
	s.False(ok)
}

func (s *StoreSuite) TestCancelOutcomes() {
	ctx := context.Background()
	tests := []struct {
		status int16
		want   store.CancelOutcome
	}{
		{1, store.CancelOK},
		{2, store.CancelRunning},
		{3, store.CancelFinished},
		{4, store.CancelFinished},
		{5, store.CancelFinished},
	}
	for _, tt := range tests {
		id := s.insertJob(1, tt.status, 0)
		got, err := s.st.Cancel(ctx, id)
		s.Require().NoError(err)
		s.Equal(tt.want, got)
	}
	got, err := s.st.Cancel(ctx, 99999)
	s.Require().NoError(err)
	s.Equal(store.CancelNotFound, got)
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

// insertRunning inserts a running row with the given attempts and a deadline
// deadlineIn from now.
func (s *StoreSuite) insertRunning(kind int16, attempt, maxAttempts int, deadlineIn time.Duration) int64 {
	s.T().Helper()
	var id int64
	err := s.Pool.QueryRow(context.Background(), `
INSERT INTO notasap_jobs (kind, status, payload, attempt, max_attempts, lease, deadline)
VALUES ($1, 2, '{}', $2, $3, 1, NOW() + make_interval(secs => $4)) RETURNING id`,
		kind, attempt, maxAttempts, deadlineIn.Seconds()).Scan(&id)
	s.Require().NoError(err)
	return id
}

func (s *StoreSuite) TestRescue() {
	retry := s.insertRunning(1, 1, 3, -time.Minute)
	fail := s.insertRunning(7, 3, 3, -2*time.Minute)
	live := s.insertRunning(1, 1, 3, time.Hour)
	pending := s.insertJob(1, 1, -time.Second)

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
	s.Equal(int16(1), row.Status)
	s.Equal(1, row.Attempt)
	s.Equal(int64(1), row.Lease)
	s.Nil(row.Deadline)
	s.WithinDuration(time.Now().Add(time.Hour), row.RunAt, 5*time.Second)
	s.Require().NotNil(row.Error)
	s.Equal("rescued: deadline exceeded", *row.Error)

	row = s.Job(fail)
	s.Equal(int16(4), row.Status)
	s.Nil(row.Deadline)
	s.Require().NotNil(row.Error)
	s.Equal("rescued: deadline exceeded", *row.Error)

	s.Equal(int16(2), s.Job(live).Status)
	s.Equal(int16(1), s.Job(pending).Status)
}

func (s *StoreSuite) TestRescueLimitAndSkipLocked() {
	locked := s.insertRunning(1, 1, 3, -3*time.Minute)
	first := s.insertRunning(1, 1, 3, -2*time.Minute)
	s.insertRunning(1, 1, 3, -time.Minute)
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
	s.Equal(int16(1), s.Job(first).Status)
}

func (s *StoreSuite) TestRescueNothing() {
	s.insertRunning(1, 1, 3, time.Hour)
	n, err := s.st.Rescue(context.Background(), 10, func(store.StuckJob) store.RescueDecision {
		s.Fail("decide called")
		return store.RescueDecision{}
	})
	s.Require().NoError(err)
	s.Zero(n)
}

func (s *StoreSuite) TestDeleteFinished() {
	insert := func(status int16, age time.Duration) int64 {
		var id int64
		s.Require().NoError(s.Pool.QueryRow(context.Background(), `
INSERT INTO notasap_jobs (kind, status, payload, max_attempts, updated_at)
VALUES (1, $1, '{}', 3, NOW() - make_interval(secs => $2)) RETURNING id`,
			status, age.Seconds()).Scan(&id))
		return id
	}
	exists := func(id int64) bool {
		var ok bool
		s.Require().NoError(s.Pool.QueryRow(context.Background(),
			"SELECT EXISTS (SELECT 1 FROM notasap_jobs WHERE id = $1)", id).Scan(&ok))
		return ok
	}
	oldest := insert(3, 5*time.Hour)
	older := insert(3, 4*time.Hour)
	old := insert(3, 3*time.Hour)
	fresh := insert(3, time.Minute)
	failed := insert(4, 5*time.Hour)
	pending := insert(1, 5*time.Hour)

	ctx := context.Background()
	n, err := s.st.DeleteFinished(ctx, 3, time.Hour, 2)
	s.Require().NoError(err)
	s.Equal(int64(2), n)
	s.False(exists(oldest))
	s.False(exists(older))
	s.True(exists(old))

	n, err = s.st.DeleteFinished(ctx, 3, time.Hour, 2)
	s.Require().NoError(err)
	s.Equal(int64(1), n)
	s.False(exists(old))

	n, err = s.st.DeleteFinished(ctx, 3, time.Hour, 2)
	s.Require().NoError(err)
	s.Zero(n)
	s.True(exists(fresh))
	s.True(exists(failed))
	s.True(exists(pending))
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
	s.Exec("UPDATE notasap_jobs SET status = 3 WHERE id = $1", seed[1].ID)
	s.Exec("UPDATE notasap_jobs SET status = 2 WHERE id = $1", seed[2].ID)

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
	s.Equal(int16(1), s.Job(seed[1].ID).Status)
	s.Equal(int16(2), s.Job(seed[2].ID).Status)
}
