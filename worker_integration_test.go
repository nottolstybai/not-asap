package notasap_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

type WorkerSuite struct {
	clientSuite
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(WorkerSuite))
}

func (s *WorkerSuite) TestOutcome() {
	s.Run("done", func() {
		c := s.startClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{N: 1})

		row := s.waitStatus(id, notasap.StatusDone)
		s.Equal(1, row.Attempt)
		s.Nil(row.Error)
		s.Nil(row.Deadline)
		s.stop(c)
		s.Equal(1, c.obs.Count("finished/done"))
	})

	s.Run("error retries then fails", func() {
		var calls atomic.Int64
		c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
			calls.Add(1)
			return errors.New("boom")
		}, notasap.KindConfig{Name: "a", MaxAttempts: 3})})
		id := s.schedule(c, payloadA{})

		row := s.waitStatus(id, notasap.StatusFailed)
		s.Equal(3, row.Attempt)
		s.Require().NotNil(row.Error)
		s.Equal("boom", *row.Error)
		s.EqualValues(3, calls.Load())
	})

	s.Run("panic retried", func() {
		c := s.startClient([]kindReg{reg(func(_ context.Context, j *notasap.Job[payloadA]) error {
			if j.Attempt == 1 {
				panic("kaboom")
			}
			return nil
		}, notasap.KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})

		row := s.waitStatus(id, notasap.StatusDone)
		s.Equal(2, row.Attempt)
		s.stop(c)
		s.Equal(1, c.obs.Count("retried/panic"))
	})

	s.Run("timeout retried", func() {
		c := s.startClient([]kindReg{reg(func(ctx context.Context, _ *notasap.Job[payloadA]) error {
			<-ctx.Done()
			return ctx.Err()
		}, notasap.KindConfig{Name: "a", Timeout: 20 * time.Millisecond, MaxAttempts: 2})})
		id := s.schedule(c, payloadA{})

		row := s.waitStatus(id, notasap.StatusFailed)
		s.Equal(2, row.Attempt)
		s.stop(c)
		s.Equal(1, c.obs.Count("retried/timeout"))
		s.Equal(1, c.obs.Count("finished/failed"))
	})

	s.Run("ErrCancel cancels", func() {
		c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
			return fmt.Errorf("give up: %w", notasap.ErrCancel)
		}, notasap.KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})

		row := s.waitStatus(id, notasap.StatusCancelled)
		s.Require().NotNil(row.Error)
		s.Contains(*row.Error, "cancel")
		s.Equal(1, row.Attempt)
	})

	s.Run("ErrKeepAttempt keeps attempt", func() {
		var calls atomic.Int64
		c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
			if calls.Add(1) <= 3 {
				return fmt.Errorf("not yet: %w", notasap.ErrKeepAttempt)
			}
			return nil
		}, notasap.KindConfig{Name: "a", MaxAttempts: 2})})
		id := s.schedule(c, payloadA{})

		row := s.waitStatus(id, notasap.StatusDone)
		s.Equal(1, row.Attempt)
		s.stop(c)
		s.Equal(3, c.obs.Count("retried/keep_attempt"))
		s.EqualValues(4, calls.Load())
	})

	s.Run("bad payload fails without handler", func() {
		var called atomic.Bool
		s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
			called.Store(true)
			return nil
		}, notasap.KindConfig{Name: "a"})})
		id := s.InsertJob(pgtest.WithKind(kindA), pgtest.WithPayload(`{"n":"str"}`))

		row := s.waitStatus(id, notasap.StatusFailed)
		s.Equal(1, row.Attempt)
		s.Require().NotNil(row.Error)
		s.True(strings.HasPrefix(*row.Error, "decode payload"), *row.Error)
		s.False(called.Load())
	})

	s.Run("long error stored as valid UTF-8", func() {
		c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
			return errors.New(strings.Repeat("я", 3000))
		}, notasap.KindConfig{Name: "a", MaxAttempts: 1})})
		id := s.schedule(c, payloadA{})

		row := s.waitStatus(id, notasap.StatusFailed)
		s.Require().NotNil(row.Error)
		s.True(utf8.ValidString(*row.Error))
		s.LessOrEqual(len(*row.Error), 4096)
		s.Greater(len(*row.Error), 4000)
	})
}

func (s *WorkerSuite) TestDelayedNotRunEarly() {
	c := s.startClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a"})})
	delayed := s.schedule(c, payloadA{N: 1}, notasap.WithDelay(time.Hour))
	now := s.schedule(c, payloadA{N: 2})

	s.waitStatus(now, notasap.StatusDone)
	row := s.Job(delayed)
	s.Equal(notasap.StatusPending, notasap.Status(row.Status))
	s.Equal(0, row.Attempt)
}

func (s *WorkerSuite) TestTxScheduledVisibleOnlyAfterCommit() {
	ctx := context.Background()
	var called atomic.Bool
	c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
		called.Store(true)
		return nil
	}, notasap.KindConfig{Name: "a"})})

	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback(ctx) }()
	id, err := c.ScheduleTx(ctx, tx, payloadA{})
	s.Require().NoError(err)
	// A claim from another connection must not see the uncommitted row.
	other := store.New(pgtest.NewPool(s.T(), s.Schema))
	jobs, err := other.Claim(ctx, int16(kindA), 10, time.Minute)
	s.Require().NoError(err)
	s.Empty(jobs)
	s.False(called.Load())

	s.Require().NoError(tx.Commit(ctx))
	s.waitStatus(id, notasap.StatusDone)
	s.True(called.Load())
}

func (s *WorkerSuite) TestKindIsolation() {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 1)
	c := s.startClient([]kindReg{
		reg(func(context.Context, *notasap.Job[payloadA]) error {
			started <- struct{}{}
			<-release
			return nil
		}, notasap.KindConfig{Name: "a", Workers: 1, Timeout: 10 * time.Minute}),
		reg(noop[payloadB], notasap.KindConfig{Name: "b"}),
	}, notasap.WithRescuer(notasap.RescuerConfig{Grace: 5 * time.Minute}))
	blocked := s.schedule(c, payloadA{})
	recv(s.T(), started)

	// The claim's lease is Timeout + Grace, measured on the DB clock.
	var leaseLeft float64
	s.Require().NoError(s.Pool.QueryRow(context.Background(),
		`SELECT EXTRACT(EPOCH FROM deadline - NOW())::float8 FROM notasap_jobs WHERE id = $1`, blocked).Scan(&leaseLeft))
	s.InDelta((15 * time.Minute).Seconds(), leaseLeft, 2)

	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = s.schedule(c, payloadB{Msg: fmt.Sprint(i)})
	}
	for _, id := range ids {
		s.waitStatus(id, notasap.StatusDone)
	}
	s.Equal(notasap.StatusRunning, s.status(blocked))
}

func (s *WorkerSuite) TestTwoClientsNoDoubleExecution() {
	const n = 200
	var seen sync.Map
	// Each client's first handler call waits until both clients have run
	// one, so neither can drain the queue alone: Workers bounds the claim.
	var ready sync.WaitGroup
	ready.Add(2)
	bothReady := make(chan struct{})
	go func() { ready.Wait(); close(bothReady) }()
	handler := func(processed *atomic.Int64) notasap.Handler[payloadA] {
		arrived := sync.OnceFunc(ready.Done)
		return func(ctx context.Context, j *notasap.Job[payloadA]) error {
			cnt, _ := seen.LoadOrStore(j.ID, new(atomic.Int64))
			cnt.(*atomic.Int64).Add(1)
			processed.Add(1)
			arrived()
			select {
			case <-bothReady:
			case <-ctx.Done():
				return ctx.Err()
			}
			time.Sleep(2 * time.Millisecond) // simulated work
			return nil
		}
	}
	var processed1, processed2 atomic.Int64
	cfg := notasap.KindConfig{Name: "a", Workers: 2}
	c1 := s.startClient([]kindReg{reg(handler(&processed1), cfg)})
	s.startClientOn(pgtest.NewPool(s.T(), s.Schema), []kindReg{reg(handler(&processed2), cfg)})

	items := make([]notasap.BatchItem, n)
	for i := range items {
		items[i] = notasap.Item(payloadA{N: i})
	}
	_, err := c1.ScheduleBatch(context.Background(), items...)
	s.Require().NoError(err)

	s.WaitFor(func() bool { return s.CountJobs(store.StatusDone) == n }, "not all jobs done")

	var total int
	seen.Range(func(_, v any) bool {
		total++
		s.EqualValues(1, v.(*atomic.Int64).Load())
		return true
	})
	s.Equal(n, total)
	s.Positive(processed1.Load())
	s.Positive(processed2.Load())
	s.EqualValues(n, processed1.Load()+processed2.Load())
}

func (s *WorkerSuite) TestStop() {
	s.Run("drains running handler", func() {
		started := make(chan struct{}, 1)
		c := s.startClient([]kindReg{reg(func(context.Context, *notasap.Job[payloadA]) error {
			started <- struct{}{}
			time.Sleep(100 * time.Millisecond) // simulated work that Stop must wait for
			return nil
		}, notasap.KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})
		recv(s.T(), started)

		s.stop(c)
		s.Equal(notasap.StatusDone, s.status(id))
	})

	s.Run("timeout refunds attempt", func() {
		started := make(chan struct{}, 1)
		c := s.startClient([]kindReg{reg(func(ctx context.Context, _ *notasap.Job[payloadA]) error {
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}, notasap.KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})
		recv(s.T(), started)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		s.Require().ErrorIs(c.Stop(ctx), context.DeadlineExceeded)

		row := s.Job(id)
		s.Equal(notasap.StatusPending, notasap.Status(row.Status))
		s.Equal(0, row.Attempt)
		s.False(row.RunAt.After(s.DBNow()))
		s.Equal(1, c.obs.Count("retried/shutdown"))
	})
}

func (s *WorkerSuite) TestStartFailsWithoutTable() {
	schema := "empty_" + strings.ToLower(strings.ReplaceAll(s.Schema, "nt_", ""))
	s.Exec("CREATE SCHEMA " + schema)
	s.ExecOnCleanup("DROP SCHEMA " + schema + " CASCADE")

	c, err := notasap.New(pgtest.NewPool(s.T(), schema), notasap.WithLogger(slog.New(slog.DiscardHandler)))
	s.Require().NoError(err)
	err = c.Start(context.Background())
	s.Require().ErrorIs(err, store.ErrNoTable)
	s.Require().NoError(c.Stop(context.Background()))
}

func (s *WorkerSuite) TestScheduleWakesIdlePoller() {
	c := s.startClient([]kindReg{reg(noop[payloadA], notasap.KindConfig{Name: "a", PollInterval: time.Hour})})
	// The first poll finds nothing, so the poller sleeps for PollInterval.
	s.Eventually(func() bool { return c.obs.Count("polled/a") >= 1 }, waitFor, tick)
	id := s.schedule(c, payloadA{})

	s.waitStatus(id, notasap.StatusDone)
	s.Equal(2, c.obs.Count("polled/a"))
}
