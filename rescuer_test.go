package notasap

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type rescuerSuite struct {
	testSuite
}

func TestRescuer(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(rescuerSuite))
}

// TestClient_rescue drives an unstarted client: no background pass can race
// with rescue.
func (s *rescuerSuite) TestClient_rescue() {
	s.Run("retries with kind backoff", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], KindConfig{
			Name:    "a",
			Backoff: Constant(time.Hour),
		})})
		id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        new(int16(kindA)),
			Status:      new(store.StatusRunning),
			DeadlineIn:  new(-time.Minute),
			Attempt:     new(1),
			MaxAttempts: new(3),
		}).ID

		c.rescue(s.ctx)
		row := s.GetJob(id)
		s.Equal(StatusPending, Status(row.Status))
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)
		s.Equal(1, row.Attempt)
		s.Nil(row.Deadline)
		s.WithinDuration(s.DBNow().Add(time.Hour), row.RunAt, 5*time.Second)
		s.Equal(int64(1), row.Lease)
		s.Equal(1, c.obs.Count("rescued/a"))
	})

	s.Run("fails without attempts left", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], KindConfig{Name: "a"})})
		id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        new(int16(kindA)),
			Status:      new(store.StatusRunning),
			DeadlineIn:  new(-time.Minute),
			Attempt:     new(3),
			MaxAttempts: new(3),
		}).ID

		c.rescue(s.ctx)
		row := s.GetJob(id)
		s.Equal(StatusFailed, Status(row.Status))
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)
		s.Equal(3, row.Attempt)
		s.Nil(row.Deadline)
		s.Equal(1, c.obs.Count("rescued/a"))
	})

	s.Run("ignores live deadline", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], KindConfig{Name: "a"})})
		id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        new(int16(kindA)),
			Status:      new(store.StatusRunning),
			DeadlineIn:  new(time.Hour),
			Attempt:     new(1),
			MaxAttempts: new(3),
		}).ID
		before := s.GetJob(id)

		c.rescue(s.ctx)
		s.Equal(before, s.GetJob(id))
		s.Equal(0, c.obs.Count("rescued/a"))
	})

	s.Run("unknown kind uses client defaults", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], KindConfig{
			Name:    "a",
			Backoff: Constant(time.Hour),
		})},
			WithDefaults(KindConfig{Backoff: Constant(2 * time.Hour)}))
		id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:        new(int16(unknownPayload{}.Kind())),
			Status:      new(store.StatusRunning),
			DeadlineIn:  new(-time.Minute),
			Attempt:     new(1),
			MaxAttempts: new(3),
		}).ID

		c.rescue(s.ctx)
		row := s.GetJob(id)
		s.Equal(StatusPending, Status(row.Status))
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)
		s.WithinDuration(s.DBNow().Add(2*time.Hour), row.RunAt, 5*time.Second)
		s.Equal(1, c.obs.Count("rescued/kind_99"))
	})
}

// TestClient_rescueLoop covers the real path end to end: a claim's lease is
// Timeout + Grace, the handler outlives it, and the background loop (not
// rescue) returns the job to pending.
func (s *rescuerSuite) TestClient_rescueLoop() {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
		started <- struct{}{}
		<-release // ignores its context
		return nil
	}, KindConfig{
		Name:        "a",
		Workers:     1,
		Timeout:     50 * time.Millisecond,
		MaxAttempts: 3,
		Backoff:     Constant(time.Hour),
	})}, WithRescuer(RescuerConfig{
		Interval: 20 * time.Millisecond,
		Grace:    50 * time.Millisecond,
	}))
	unblock := sync.OnceFunc(func() { close(release) })
	s.T().Cleanup(unblock) // registered after startClient, so it runs before the client's Stop
	id := s.schedule(c, payloadA{})
	recv(s.T(), started)

	pgtest.WaitStatus(&s.Suite, id, StatusPending)
	unblock()
	s.stop(c) // drains the stale handler and its fenced write
	row := s.GetJob(id)
	s.Equal(StatusPending, Status(row.Status))
	s.Require().NotNil(row.Error)
	s.Equal(store.RescuedError, *row.Error)
	s.Equal(1, row.Attempt)
	s.Equal(1, c.obs.Count("rescued/a"))
	s.Equal(0, c.obs.Count("finished/done"))
}

// TestClient_rescue_StaleWorker needs running handlers, so the client is
// started. Its claims get a deadline an hour away; the test expires it with
// SetJob and calls rescue. The Start pass only sees rows already past their
// deadline, so at worst it performs that same rescue: the subtests wait for
// the rescued status rather than reading it right after rescue.
func (s *rescuerSuite) TestClient_rescue_StaleWorker() {
	s.Run("write fenced after rescue", func() {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
			started <- struct{}{}
			<-release
			return nil
		}, KindConfig{
			Name:        "a",
			Workers:     1,
			Timeout:     time.Hour,
			MaxAttempts: 3,
			Backoff:     Constant(time.Hour),
		})})
		unblock := sync.OnceFunc(func() { close(release) })
		s.T().Cleanup(unblock) // registered after startClient, so it runs before the client's Stop
		id := s.schedule(c, payloadA{})
		recv(s.T(), started)

		s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{DeadlineIn: new(-time.Minute)})
		c.rescue(s.ctx)
		row := pgtest.WaitStatus(&s.Suite, id, StatusPending)
		s.Equal(1, row.Attempt)
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)

		unblock()
		s.stop(c) // drains the stale handler, including its fenced status write
		s.Equal(StatusPending, Status(s.GetJob(id).Status))
		s.Equal(0, c.obs.Count("finished/done"))
		s.Equal(1, c.obs.Count("rescued/a"))
	})

	s.Run("write fenced by new lease", func() {
		logs := &syncBuffer{}
		startedStale := make(chan struct{}, 1)
		startedSecond := make(chan struct{}, 1)
		releaseStale := make(chan struct{})
		releaseSecond := make(chan struct{})
		h := func(_ context.Context, j *Job[payloadA]) error {
			if j.Attempt == 1 {
				startedStale <- struct{}{}
				<-releaseStale
				return errors.New("stale failure")
			}
			startedSecond <- struct{}{}
			<-releaseSecond
			return nil
		}
		c := s.startClient([]kindReg{reg(h, KindConfig{
			Name:        "a",
			Workers:     2,
			Timeout:     time.Hour,
			MaxAttempts: 3,
			Backoff:     Constant(0),
		})}, WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
		unblockStale := sync.OnceFunc(func() { close(releaseStale) })
		unblockSecond := sync.OnceFunc(func() { close(releaseSecond) })
		s.T().Cleanup(unblockStale) // registered after startClient, so they run before the client's Stop
		s.T().Cleanup(unblockSecond)
		id := s.schedule(c, payloadA{})
		recv(s.T(), startedStale)

		s.Fixture.SetJob(s.ctx, id, pgtest.JobOpt{DeadlineIn: new(-time.Minute)})
		c.rescue(s.ctx)
		// The rescued job is due at once (backoff 0) and claimed again with
		// lease 2; its new deadline is an hour away, so no further rescue.
		recv(s.T(), startedSecond)
		row := s.GetJob(id)
		s.Equal(StatusRunning, Status(row.Status))
		s.Equal(int64(2), row.Lease)
		s.Equal(2, row.Attempt)

		unblockStale()
		s.WaitFor(func() bool { return strings.Contains(logs.String(), "job lost lease") }, "no lost lease warning")
		row = s.GetJob(id)
		s.Equal(StatusRunning, Status(row.Status))
		s.Equal(int64(2), row.Lease)
		s.Equal(2, row.Attempt)

		unblockSecond()
		row = pgtest.WaitStatus(&s.Suite, id, StatusDone)
		s.Equal(2, row.Attempt)
		s.Equal(int64(2), row.Lease)
		s.stop(c)
		s.Equal(1, c.obs.Count("finished/done"))
		s.Equal(0, c.obs.Count("retried/error"))
		s.Equal(1, c.obs.Count("rescued/a"))
	})
}
