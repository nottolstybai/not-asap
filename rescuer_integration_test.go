package notasap_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type RescuerSuite struct {
	clientSuite
}

func TestRescuerSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(RescuerSuite))
}

// TestRescueNow drives an unstarted client: no background pass can race
// with RescueNow.
func (s *RescuerSuite) TestRescueNow() {
	s.Run("retries with kind backoff", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a", Backoff: notasap.Constant(time.Hour)})})
		id := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(kindA)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-time.Minute),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		}).ID

		c.RescueNow(context.Background())
		row := s.GetJob(id)
		s.Equal(notasap.StatusPending, notasap.Status(row.Status))
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)
		s.Equal(1, row.Attempt)
		s.Nil(row.Deadline)
		s.WithinDuration(s.DBNow().Add(time.Hour), row.RunAt, 5*time.Second)
		s.Equal(int64(1), row.Lease)
		s.Equal(1, c.obs.Count("rescued/a"))
	})

	s.Run("fails without attempts left", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a"})})
		id := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(kindA)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-time.Minute),
			Attempt:     lo.ToPtr(3),
			MaxAttempts: lo.ToPtr(3),
		}).ID

		c.RescueNow(context.Background())
		row := s.GetJob(id)
		s.Equal(notasap.StatusFailed, notasap.Status(row.Status))
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)
		s.Equal(3, row.Attempt)
		s.Nil(row.Deadline)
		s.Equal(1, c.obs.Count("rescued/a"))
	})

	s.Run("ignores live deadline", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a"})})
		id := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(kindA)),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(time.Hour),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		}).ID
		before := s.GetJob(id)

		c.RescueNow(context.Background())
		s.Equal(before, s.GetJob(id))
		s.Equal(0, c.obs.Count("rescued/a"))
	})

	s.Run("unknown kind uses client defaults", func() {
		c := s.newClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a", Backoff: notasap.Constant(time.Hour)})},
			notasap.WithDefaults(notasap.KindConfig{Backoff: notasap.Constant(2 * time.Hour)}))
		id := s.Fixture.Job(context.Background(), pgtest.JobOpt{
			Kind:        lo.ToPtr(int16(unregisteredPayload{}.Kind())),
			Status:      lo.ToPtr(store.StatusRunning),
			DeadlineIn:  lo.ToPtr(-time.Minute),
			Attempt:     lo.ToPtr(1),
			MaxAttempts: lo.ToPtr(3),
		}).ID

		c.RescueNow(context.Background())
		row := s.GetJob(id)
		s.Equal(notasap.StatusPending, notasap.Status(row.Status))
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)
		s.WithinDuration(s.DBNow().Add(2*time.Hour), row.RunAt, 5*time.Second)
		s.Equal(1, c.obs.Count("rescued/kind_99"))
	})
}

// TestBackgroundRescue covers the real path end to end: a claim's lease is
// Timeout + Grace, the handler outlives it, and the background loop (not
// RescueNow) returns the job to pending.
func (s *RescuerSuite) TestBackgroundRescue() {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
		started <- struct{}{}
		<-release // ignores its context
		return nil
	}, notasap.KindConfig{
		Name: "a", Workers: 1, Timeout: 50 * time.Millisecond, MaxAttempts: 3,
		Backoff: notasap.Constant(time.Hour),
	})}, notasap.WithRescuer(notasap.RescuerConfig{Interval: 20 * time.Millisecond, Grace: 50 * time.Millisecond}))
	unblock := sync.OnceFunc(func() { close(release) })
	s.T().Cleanup(unblock) // registered after startClient, so it runs before the client's Stop
	id := s.schedule(c, payloadA{})
	recv(s.T(), started)

	s.waitStatus(id, notasap.StatusPending)
	unblock()
	s.stop(c) // drains the stale handler and its fenced write
	row := s.GetJob(id)
	s.Equal(notasap.StatusPending, notasap.Status(row.Status))
	s.Require().NotNil(row.Error)
	s.Equal(store.RescuedError, *row.Error)
	s.Equal(1, row.Attempt)
	s.Equal(1, c.obs.Count("rescued/a"))
	s.Equal(0, c.obs.Count("finished/done"))
}

// TestStaleWorker needs running handlers, so the client is started. Its
// claims get a deadline an hour away; the test expires it with SetJob and
// rescues with RescueNow. The Start pass only sees rows already past their
// deadline, so at worst it performs that same rescue: the subtests wait for
// the rescued status rather than reading it right after RescueNow.
func (s *RescuerSuite) TestStaleWorker() {
	s.Run("write fenced after rescue", func() {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
			started <- struct{}{}
			<-release
			return nil
		}, notasap.KindConfig{
			Name: "a", Workers: 1, Timeout: time.Hour, MaxAttempts: 3,
			Backoff: notasap.Constant(time.Hour),
		})})
		unblock := sync.OnceFunc(func() { close(release) })
		s.T().Cleanup(unblock) // registered after startClient, so it runs before the client's Stop
		id := s.schedule(c, payloadA{})
		recv(s.T(), started)

		s.Fixture.SetJob(context.Background(), id, pgtest.JobOpt{DeadlineIn: lo.ToPtr(-time.Minute)})
		c.RescueNow(context.Background())
		row := s.waitStatus(id, notasap.StatusPending)
		s.Equal(1, row.Attempt)
		s.Require().NotNil(row.Error)
		s.Equal(store.RescuedError, *row.Error)

		unblock()
		s.stop(c) // drains the stale handler, including its fenced status write
		s.Equal(notasap.StatusPending, s.status(id))
		s.Equal(0, c.obs.Count("finished/done"))
		s.Equal(1, c.obs.Count("rescued/a"))
	})

	s.Run("write fenced by new lease", func() {
		logs := &syncBuffer{}
		startedStale := make(chan struct{}, 1)
		startedSecond := make(chan struct{}, 1)
		releaseStale := make(chan struct{})
		releaseSecond := make(chan struct{})
		h := func(_ context.Context, j *notasap.Job[payloadA]) error {
			if j.Attempt == 1 {
				startedStale <- struct{}{}
				<-releaseStale
				return errors.New("stale failure")
			}
			startedSecond <- struct{}{}
			<-releaseSecond
			return nil
		}
		c := s.startClient([]kindReg{reg(h, notasap.KindConfig{
			Name: "a", Workers: 2, Timeout: time.Hour, MaxAttempts: 3,
			Backoff: notasap.Constant(0),
		})}, notasap.WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
		unblockStale := sync.OnceFunc(func() { close(releaseStale) })
		unblockSecond := sync.OnceFunc(func() { close(releaseSecond) })
		s.T().Cleanup(unblockStale) // registered after startClient, so they run before the client's Stop
		s.T().Cleanup(unblockSecond)
		id := s.schedule(c, payloadA{})
		recv(s.T(), startedStale)

		s.Fixture.SetJob(context.Background(), id, pgtest.JobOpt{DeadlineIn: lo.ToPtr(-time.Minute)})
		c.RescueNow(context.Background())
		// The rescued job is due at once (backoff 0) and claimed again with
		// lease 2; its new deadline is an hour away, so no further rescue.
		recv(s.T(), startedSecond)
		row := s.GetJob(id)
		s.Equal(notasap.StatusRunning, notasap.Status(row.Status))
		s.Equal(int64(2), row.Lease)
		s.Equal(2, row.Attempt)

		unblockStale()
		s.WaitFor(func() bool { return strings.Contains(logs.String(), "job lost lease") }, "no lost lease warning")
		row = s.GetJob(id)
		s.Equal(notasap.StatusRunning, notasap.Status(row.Status))
		s.Equal(int64(2), row.Lease)
		s.Equal(2, row.Attempt)

		unblockSecond()
		row = s.waitStatus(id, notasap.StatusDone)
		s.Equal(2, row.Attempt)
		s.Equal(int64(2), row.Lease)
		s.stop(c)
		s.Equal(1, c.obs.Count("finished/done"))
		s.Equal(0, c.obs.Count("retried/error"))
		s.Equal(1, c.obs.Count("rescued/a"))
	})
}
