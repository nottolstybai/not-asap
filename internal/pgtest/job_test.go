package pgtest_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

type JobSuite struct {
	pgtest.Suite
}

func TestJobSuite(t *testing.T) {
	suite.Run(t, new(JobSuite))
}

type myKind int16

func (s *JobSuite) TestInsertJobDefaults() {
	id := s.InsertJob()
	row := s.Job(id)
	s.Equal(int16(1), row.Kind)
	s.Equal(store.StatusPending, row.Status)
	s.JSONEq(`{}`, string(row.Payload))
	s.Equal(0, row.Attempt)
	s.Equal(3, row.MaxAttempts)
	s.Nil(row.Deadline)
	s.Nil(row.Key)
	s.Nil(row.Error)
	s.Zero(row.Lease)
}

func (s *JobSuite) TestInsertJobRunningPreset() {
	id := s.InsertJob(pgtest.WithKind(myKind(7)), pgtest.Running(-time.Minute))
	row := s.Job(id)
	now := s.DBNow()
	s.Equal(int16(7), row.Kind)
	s.Equal(store.StatusRunning, row.Status)
	s.Equal(1, row.Attempt)
	s.Equal(int64(1), row.Lease)
	s.Require().NotNil(row.Deadline)
	s.WithinDuration(now.Add(-time.Minute), *row.Deadline, 5*time.Second)
	s.True(row.Deadline.Before(now))
}

func (s *JobSuite) TestInsertJobOptions() {
	id := s.InsertJob(
		pgtest.WithPayload(`{"n":1}`), pgtest.WithAttempt(2, 9), pgtest.WithLease(4),
		pgtest.WithRunIn(time.Hour), pgtest.WithKey("k"), pgtest.WithError("boom"),
		pgtest.Finished(store.StatusDone, 25*time.Hour))
	row := s.Job(id)
	now := s.DBNow()
	s.JSONEq(`{"n":1}`, string(row.Payload))
	s.Equal(2, row.Attempt)
	s.Equal(9, row.MaxAttempts)
	s.Equal(int64(4), row.Lease)
	s.WithinDuration(now.Add(time.Hour), row.RunAt, 5*time.Second)
	s.Require().NotNil(row.Key)
	s.Equal("k", *row.Key)
	s.Require().NotNil(row.Error)
	s.Equal("boom", *row.Error)
	s.Equal(store.StatusDone, row.Status)
	s.WithinDuration(now.Add(-25*time.Hour), row.UpdatedAt, 5*time.Second)
}

func (s *JobSuite) TestInsertJobsTie() {
	ids := s.InsertJobs(3, pgtest.WithRunIn(-3*time.Second))
	s.Len(ids, 3)
	a, b, c := s.Job(ids[0]), s.Job(ids[1]), s.Job(ids[2])
	s.True(a.RunAt.Equal(b.RunAt))
	s.True(b.RunAt.Equal(c.RunAt))
	s.True(a.UpdatedAt.Equal(c.UpdatedAt))
	s.Equal(3, s.CountJobs())
}

func (s *JobSuite) TestSetJobTouchesOnlyGivenColumns() {
	id := s.InsertJob(pgtest.WithPayload(`{"a":1}`), pgtest.WithAttempt(1, 5),
		pgtest.WithKey("k"), pgtest.WithRunIn(time.Hour))
	before := s.Job(id)

	s.SetJob(id, pgtest.WithStatus(store.StatusFailed), pgtest.WithError("x"),
		pgtest.WithDeadlineIn(-time.Minute))
	after := s.Job(id)
	s.Equal(store.StatusFailed, after.Status)
	s.Require().NotNil(after.Error)
	s.Equal("x", *after.Error)
	s.Require().NotNil(after.Deadline)
	s.True(after.Deadline.Before(s.DBNow()))

	s.Equal(before.Kind, after.Kind)
	s.JSONEq(string(before.Payload), string(after.Payload))
	s.Equal(before.Attempt, after.Attempt)
	s.Equal(before.MaxAttempts, after.MaxAttempts)
	s.Equal(before.Lease, after.Lease)
	s.True(before.RunAt.Equal(after.RunAt))
	s.Equal(before.Key, after.Key)
	s.True(before.UpdatedAt.Equal(after.UpdatedAt))
}

func (s *JobSuite) TestExistsAndCount() {
	id := s.InsertJob()
	s.InsertJob(pgtest.WithStatus(store.StatusDone))
	s.InsertJob(pgtest.WithStatus(store.StatusFailed))
	s.True(s.Exists(id))
	s.False(s.Exists(-1))
	s.Equal(3, s.CountJobs())
	s.Equal(1, s.CountJobs(store.StatusDone))
	s.Equal(2, s.CountJobs(store.StatusDone, store.StatusFailed))
}

func (s *JobSuite) TestDBNow() {
	s.WithinDuration(time.Now(), s.DBNow(), 5*time.Second)
}

func (s *JobSuite) TestWaitFor() {
	var n atomic.Int32
	s.WaitFor(func() bool { return n.Add(1) >= 3 }, "never %d", 1)
	s.GreaterOrEqual(n.Load(), int32(3))
}

func (s *JobSuite) TestWaitStatus() {
	id := s.InsertJob()
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := s.Pool.Exec(context.Background(), "UPDATE notasap_jobs SET status = 3 WHERE id = $1", id)
		s.NoError(err)
	}()
	row := pgtest.WaitStatus(&s.Suite, id, myStatus(3))
	s.Equal(id, row.ID)
}

type myStatus int16

func (s *JobSuite) TestConsistently() {
	id := s.InsertJob()
	s.Consistently(100*time.Millisecond, func() bool { return !s.Exists(id) }, "job %d vanished", id)
}
