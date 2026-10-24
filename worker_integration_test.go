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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"

	notasap "github.com/nottolstybai/not-asap"
	"github.com/nottolstybai/not-asap/internal/pgtest"
)

type WorkerSuite struct {
	pgtest.Suite
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(WorkerSuite))
}

const (
	waitFor = 10 * time.Second
	tick    = 10 * time.Millisecond
)

// workerObserver counts finished and retried events.
type workerObserver struct {
	notasap.NopObserver
	mu       sync.Mutex
	finished map[notasap.Status]int
	retried  map[notasap.RetryReason]int
}

func newWorkerObserver() *workerObserver {
	return &workerObserver{finished: make(map[notasap.Status]int), retried: make(map[notasap.RetryReason]int)}
}

func (o *workerObserver) JobFinished(_ string, st notasap.Status, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finished[st]++
}

func (o *workerObserver) JobRetried(_ string, r notasap.RetryReason, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retried[r]++
}

func (o *workerObserver) Finished(st notasap.Status) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.finished[st]
}

func (o *workerObserver) Retried(r notasap.RetryReason) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.retried[r]
}

// kindReg registers one kind on a client.
type kindReg func(c *notasap.Client) error

func reg[T notasap.Payload](h notasap.Handler[T], cfg notasap.KindConfig) kindReg {
	return func(c *notasap.Client) error { return notasap.Register(c, h, cfg) }
}

// startClient creates a client over the suite pool with obs, registers kinds,
// starts it and stops it on test cleanup.
func (s *WorkerSuite) startClient(obs notasap.Observer, kinds ...kindReg) *notasap.Client {
	return s.startClientOn(s.Pool, obs, kinds...)
}

func (s *WorkerSuite) startClientOn(pool *pgxpool.Pool, obs notasap.Observer, kinds ...kindReg) *notasap.Client {
	s.T().Helper()
	c, err := notasap.New(pool,
		notasap.WithLogger(slog.New(slog.DiscardHandler)),
		notasap.WithObserver(obs),
		notasap.WithDefaults(notasap.KindConfig{PollInterval: 10 * time.Millisecond, Backoff: notasap.Constant(0)}),
	)
	s.Require().NoError(err)
	for _, k := range kinds {
		s.Require().NoError(k(c))
	}
	s.Require().NoError(c.Start(context.Background()))
	s.T().Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.NoError(c.Stop(ctx))
	})
	return c
}

func (s *WorkerSuite) schedule(c *notasap.Client, p notasap.Payload, opts ...notasap.ScheduleOption) int64 {
	s.T().Helper()
	id, err := c.Schedule(context.Background(), p, opts...)
	s.Require().NoError(err)
	return id
}

func (s *WorkerSuite) waitStatus(id int64, st notasap.Status) pgtest.JobRow {
	s.T().Helper()
	s.Require().Eventually(func() bool { return s.Job(id).Status == int16(st) }, waitFor, tick)
	return s.Job(id)
}

func (s *WorkerSuite) TestJobDone() {
	obs := newWorkerObserver()
	c := s.startClient(obs, reg(noop[payloadA], notasap.KindConfig{Name: "a"}))
	id := s.schedule(c, payloadA{N: 1})

	row := s.waitStatus(id, notasap.StatusDone)
	s.Equal(1, row.Attempt)
	s.Nil(row.Error)
	s.Nil(row.Deadline)
	s.Equal(1, obs.Finished(notasap.StatusDone))
}

func (s *WorkerSuite) TestRetryThenFailed() {
	var calls atomic.Int64
	c := s.startClient(newWorkerObserver(), reg(func(context.Context, *notasap.Job[payloadA]) error {
		calls.Add(1)
		return errors.New("boom")
	}, notasap.KindConfig{Name: "a", MaxAttempts: 3}))
	id := s.schedule(c, payloadA{})

	row := s.waitStatus(id, notasap.StatusFailed)
	s.Equal(3, row.Attempt)
	s.Require().NotNil(row.Error)
	s.Equal("boom", *row.Error)
	s.EqualValues(3, calls.Load())
}

func (s *WorkerSuite) TestPanicRetried() {
	obs := newWorkerObserver()
	c := s.startClient(obs, reg(func(_ context.Context, j *notasap.Job[payloadA]) error {
		if j.Attempt == 1 {
			panic("kaboom")
		}
		return nil
	}, notasap.KindConfig{Name: "a"}))
	id := s.schedule(c, payloadA{})

	row := s.waitStatus(id, notasap.StatusDone)
	s.Equal(2, row.Attempt)
	s.Equal(1, obs.Retried(notasap.RetryPanic))
}

func (s *WorkerSuite) TestTimeoutRetried() {
	obs := newWorkerObserver()
	c := s.startClient(obs, reg(func(ctx context.Context, _ *notasap.Job[payloadA]) error {
		<-ctx.Done()
		return ctx.Err()
	}, notasap.KindConfig{Name: "a", Timeout: 20 * time.Millisecond, MaxAttempts: 2}))
	id := s.schedule(c, payloadA{})

	row := s.waitStatus(id, notasap.StatusFailed)
	s.Equal(2, row.Attempt)
	s.Equal(1, obs.Retried(notasap.RetryTimeout))
	s.Equal(1, obs.Finished(notasap.StatusFailed))
}

func (s *WorkerSuite) TestErrCancel() {
	c := s.startClient(newWorkerObserver(), reg(func(context.Context, *notasap.Job[payloadA]) error {
		return fmt.Errorf("give up: %w", notasap.ErrCancel)
	}, notasap.KindConfig{Name: "a"}))
	id := s.schedule(c, payloadA{})

	row := s.waitStatus(id, notasap.StatusCancelled)
	s.Require().NotNil(row.Error)
	s.Contains(*row.Error, "cancel")
	s.Equal(1, row.Attempt)
}

func (s *WorkerSuite) TestKeepAttempt() {
	obs := newWorkerObserver()
	var calls atomic.Int64
	c := s.startClient(obs, reg(func(context.Context, *notasap.Job[payloadA]) error {
		if calls.Add(1) <= 3 {
			return fmt.Errorf("not yet: %w", notasap.ErrKeepAttempt)
		}
		return nil
	}, notasap.KindConfig{Name: "a", MaxAttempts: 2}))
	id := s.schedule(c, payloadA{})

	row := s.waitStatus(id, notasap.StatusDone)
	s.Equal(1, row.Attempt)
	s.Equal(3, obs.Retried(notasap.RetryKeepAttempt))
	s.EqualValues(4, calls.Load())
}

func (s *WorkerSuite) TestBadPayload() {
	var called atomic.Bool
	s.startClient(newWorkerObserver(), reg(func(context.Context, *notasap.Job[payloadA]) error {
		called.Store(true)
		return nil
	}, notasap.KindConfig{Name: "a"}))
	var id int64
	s.Require().NoError(s.Pool.QueryRow(context.Background(),
		`INSERT INTO notasap_jobs (kind, status, payload, max_attempts) VALUES ($1, 1, '{"n":"str"}', 3) RETURNING id`,
		int16(kindA)).Scan(&id))

	row := s.waitStatus(id, notasap.StatusFailed)
	s.Equal(1, row.Attempt)
	s.Require().NotNil(row.Error)
	s.True(strings.HasPrefix(*row.Error, "decode payload"), *row.Error)
	s.False(called.Load())
}

func (s *WorkerSuite) TestDelayedNotRunEarly() {
	c := s.startClient(newWorkerObserver(), reg(noop[payloadA], notasap.KindConfig{Name: "a"}))
	delayed := s.schedule(c, payloadA{N: 1}, notasap.WithDelay(time.Hour))
	now := s.schedule(c, payloadA{N: 2})

	s.waitStatus(now, notasap.StatusDone)
	s.Equal(int16(notasap.StatusPending), s.Job(delayed).Status)
	s.Equal(0, s.Job(delayed).Attempt)
}

func (s *WorkerSuite) TestHandlerErrorUTF8Stored() {
	c := s.startClient(newWorkerObserver(), reg(func(context.Context, *notasap.Job[payloadA]) error {
		return errors.New(strings.Repeat("я", 3000))
	}, notasap.KindConfig{Name: "a", MaxAttempts: 1}))
	id := s.schedule(c, payloadA{})

	row := s.waitStatus(id, notasap.StatusFailed)
	s.Require().NotNil(row.Error)
	s.True(utf8.ValidString(*row.Error))
	s.LessOrEqual(len(*row.Error), 4096)
	s.Greater(len(*row.Error), 4000)
}

func (s *WorkerSuite) TestTxScheduledVisibleOnlyAfterCommit() {
	ctx := context.Background()
	var called atomic.Bool
	c := s.startClient(newWorkerObserver(), reg(func(context.Context, *notasap.Job[payloadA]) error {
		called.Store(true)
		return nil
	}, notasap.KindConfig{Name: "a"}))

	tx, err := s.Pool.Begin(ctx)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback(ctx) }()
	id, err := c.ScheduleTx(ctx, tx, payloadA{})
	s.Require().NoError(err)
	s.Never(called.Load, 100*time.Millisecond, tick)

	s.Require().NoError(tx.Commit(ctx))
	s.waitStatus(id, notasap.StatusDone)
	s.True(called.Load())
}

func (s *WorkerSuite) TestKindIsolation() {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 1)
	c := s.startClient(newWorkerObserver(),
		reg(func(context.Context, *notasap.Job[payloadA]) error {
			started <- struct{}{}
			<-release
			return nil
		}, notasap.KindConfig{Name: "a", Workers: 1}),
		reg(noop[payloadB], notasap.KindConfig{Name: "b"}),
	)
	blocked := s.schedule(c, payloadA{})
	<-started

	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = s.schedule(c, payloadB{Msg: fmt.Sprint(i)})
	}
	for _, id := range ids {
		s.waitStatus(id, notasap.StatusDone)
	}
	s.Equal(int16(notasap.StatusRunning), s.Job(blocked).Status)
}

func (s *WorkerSuite) TestTwoClientsNoDoubleExecution() {
	const n = 200
	var seen sync.Map
	handler := func(processed *atomic.Int64) notasap.Handler[payloadA] {
		return func(_ context.Context, j *notasap.Job[payloadA]) error {
			cnt, _ := seen.LoadOrStore(j.ID, new(atomic.Int64))
			cnt.(*atomic.Int64).Add(1)
			processed.Add(1)
			t := time.NewTimer(2 * time.Millisecond)
			defer t.Stop()
			<-t.C
			return nil
		}
	}
	var processed1, processed2 atomic.Int64
	cfg := notasap.KindConfig{Name: "a", Workers: 2}
	c1 := s.startClient(newWorkerObserver(), reg(handler(&processed1), cfg))
	s.startClientOn(pgtest.NewPool(s.T(), s.Schema), newWorkerObserver(), reg(handler(&processed2), cfg))

	items := make([]notasap.BatchItem, n)
	for i := range items {
		items[i] = notasap.Item(payloadA{N: i})
	}
	_, err := c1.ScheduleBatch(context.Background(), items...)
	s.Require().NoError(err)

	s.Require().Eventually(func() bool {
		var done int
		err := s.Pool.QueryRow(context.Background(),
			"SELECT count(*) FROM notasap_jobs WHERE status = 3").Scan(&done)
		return err == nil && done == n
	}, waitFor, tick)

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

func (s *WorkerSuite) TestStopDrains() {
	started := make(chan struct{}, 1)
	c := s.startClient(newWorkerObserver(), reg(func(context.Context, *notasap.Job[payloadA]) error {
		started <- struct{}{}
		t := time.NewTimer(100 * time.Millisecond)
		defer t.Stop()
		<-t.C
		return nil
	}, notasap.KindConfig{Name: "a"}))
	id := s.schedule(c, payloadA{})
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Require().NoError(c.Stop(ctx))
	s.Equal(int16(notasap.StatusDone), s.Job(id).Status)
}

func (s *WorkerSuite) TestStopTimeoutRefundsAttempt() {
	obs := newWorkerObserver()
	started := make(chan struct{}, 1)
	c := s.startClient(obs, reg(func(ctx context.Context, _ *notasap.Job[payloadA]) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}, notasap.KindConfig{Name: "a"}))
	id := s.schedule(c, payloadA{})
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	s.Require().ErrorIs(c.Stop(ctx), context.DeadlineExceeded)

	row := s.Job(id)
	s.Equal(int16(notasap.StatusPending), row.Status)
	s.Equal(0, row.Attempt)
	var now time.Time
	s.Require().NoError(s.Pool.QueryRow(context.Background(), "SELECT now()").Scan(&now))
	s.False(row.RunAt.After(now))
	s.Equal(1, obs.Retried(notasap.RetryShutdown))
}

func (s *WorkerSuite) TestStartFailsWithoutTable() {
	schema := "empty_" + strings.ToLower(strings.ReplaceAll(s.Schema, "nt_", ""))
	s.Exec("CREATE SCHEMA " + schema)
	s.T().Cleanup(func() { s.Exec("DROP SCHEMA " + schema + " CASCADE") })

	c, err := notasap.New(pgtest.NewPool(s.T(), schema), notasap.WithLogger(slog.New(slog.DiscardHandler)))
	s.Require().NoError(err)
	err = c.Start(context.Background())
	s.Require().ErrorContains(err, "apply migrations")
	s.Require().NoError(c.Stop(context.Background()))
}
