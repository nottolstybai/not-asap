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

type jobSuite struct {
	pgtest.Suite
	ctx context.Context
}

func TestJob(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(jobSuite))
}

func (s *jobSuite) SetupTest() {
	s.Suite.SetupTest()
	s.ctx = s.T().Context()
}

type myKind int16

func (s *jobSuite) TestFixture_Job() {
	id := s.Fixture.Job(s.ctx).ID
	row := s.GetJob(id)
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

func (s *jobSuite) TestFixture_Job_RunningPreset() {
	id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Kind:       new(int16(myKind(7))),
		Status:     new(store.StatusRunning),
		DeadlineIn: new(-time.Minute),
	}).ID
	row := s.GetJob(id)
	now := s.DBNow()
	s.Equal(int16(7), row.Kind)
	s.Equal(store.StatusRunning, row.Status)
	s.Equal(1, row.Attempt)
	s.Equal(int64(1), row.Lease)
	s.Require().NotNil(row.Deadline)
	s.WithinDuration(now.Add(-time.Minute), *row.Deadline, 5*time.Second)
	s.True(row.Deadline.Before(now))
}

func (s *jobSuite) TestFixture_Job_Options() {
	id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Payload:     new(`{"n":1}`),
		Attempt:     new(2),
		MaxAttempts: new(9),
		Lease:       new(int64(4)),
		RunIn:       new(time.Hour),
		Key:         new("k"),
		Error:       new("boom"),
		Status:      new(store.StatusDone),
		UpdatedAgo:  new(25 * time.Hour),
	}).ID
	row := s.GetJob(id)
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

func (s *jobSuite) TestFixture_Jobs() {
	rows := s.Fixture.Jobs(s.ctx, 3, pgtest.JobOpt{RunIn: new(-3 * time.Second)})
	s.Require().Len(rows, 3)
	a, b, c := s.GetJob(rows[0].ID), s.GetJob(rows[1].ID), s.GetJob(rows[2].ID)
	s.True(a.RunAt.Equal(b.RunAt))
	s.True(b.RunAt.Equal(c.RunAt))
	s.True(a.UpdatedAt.Equal(c.UpdatedAt))
	s.Equal(3, s.CountJobs())
}

func (s *jobSuite) TestFixture_SetJob() {
	id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Payload:     new(`{"a":1}`),
		Attempt:     new(1),
		MaxAttempts: new(5),
		Key:         new("k"),
		RunIn:       new(time.Hour),
	}).ID
	before := s.GetJob(id)

	s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{
		Status:     new(store.StatusFailed),
		Error:      new("x"),
		DeadlineIn: new(-time.Minute),
	})
	after := s.GetJob(id)
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

func (s *jobSuite) TestSuite_ExistsAndCountJobs() {
	id := s.Fixture.Job(s.ctx).ID
	s.Fixture.Job(s.ctx, pgtest.JobOpt{Status: new(store.StatusDone)})
	s.Fixture.Job(s.ctx, pgtest.JobOpt{Status: new(store.StatusFailed)})
	s.True(s.Exists(id))
	s.False(s.Exists(-1))
	s.Equal(3, s.CountJobs())
	s.Equal(1, s.CountJobs(store.StatusDone))
	s.Equal(2, s.CountJobs(store.StatusDone, store.StatusFailed))
}

func (s *jobSuite) TestSuite_DBNow() {
	s.WithinDuration(time.Now(), s.DBNow(), 5*time.Second)
}

func (s *jobSuite) TestSuite_WaitFor() {
	var n atomic.Int32
	s.WaitFor(func() bool { return n.Add(1) >= 3 }, "never %d", 1)
	s.GreaterOrEqual(n.Load(), int32(3))
}

func (s *jobSuite) Test_WaitStatus() {
	id := s.Fixture.Job(s.ctx).ID
	done := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := s.Pool.Exec(s.ctx, "UPDATE notasap_jobs SET status = 3 WHERE id = $1", id)
		done <- err
	}()
	row := pgtest.WaitStatus(&s.Suite, id, myStatus(3))
	s.Equal(id, row.ID)
	s.NoError(<-done)
}

func (s *jobSuite) TestFixture_SetJob_RunningKeepsOtherColumns() {
	id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Attempt:     new(2),
		MaxAttempts: new(5),
		Lease:       new(int64(4)),
	}).ID
	s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{
		Status:     new(store.StatusRunning),
		DeadlineIn: new(-time.Minute),
	})
	row := s.GetJob(id)
	s.Equal(store.StatusRunning, row.Status)
	s.Require().NotNil(row.Deadline)
	s.True(row.Deadline.Before(s.DBNow()))
	s.Equal(5, row.MaxAttempts)
	s.Equal(int64(4), row.Lease)
	s.Equal(2, row.Attempt)
}

func (s *jobSuite) TestFixture_Job_RunningDefaults() {
	row := s.GetJob(s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:     new(store.StatusRunning),
		DeadlineIn: new(time.Minute),
	}).ID)
	s.Equal(1, row.Attempt)
	s.Equal(int64(1), row.Lease)
	row = s.GetJob(s.Fixture.Job(s.ctx, pgtest.JobOpt{
		Status:      new(store.StatusRunning),
		DeadlineIn:  new(time.Minute),
		Attempt:     new(2),
		MaxAttempts: new(3),
		Lease:       new(int64(7)),
	}).ID)
	s.Equal(2, row.Attempt)
	s.Equal(int64(7), row.Lease)
}

type myStatus int16

func (s *jobSuite) TestSuite_Consistently() {
	id := s.Fixture.Job(s.ctx).ID
	s.Consistently(100*time.Millisecond, func() bool { return !s.Exists(id) }, "job %d vanished", id)
}
