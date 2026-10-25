package notasap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

const (
	waitFor = pgtest.WaitTimeout
	tick    = 5 * time.Millisecond
)

// countObserver counts JobStuck, JobEnded and JobFinished calls.
type countObserver struct {
	NopObserver
	stuck, ended, finished atomic.Int64
}

func (o *countObserver) JobStuck(string)                           { o.stuck.Add(1) }
func (o *countObserver) JobEnded(string)                           { o.ended.Add(1) }
func (o *countObserver) JobFinished(string, Status, time.Duration) { o.finished.Add(1) }

// recv waits for a value on ch or fails the test after waitFor.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(waitFor):
		t.Fatal("timed out waiting on channel")
		panic("unreachable")
	}
}

// newPoolClient returns a client over st with kind 1 registered as h.
func newPoolClient(t *testing.T, st jobStore, h Handler[testPayload], cfg KindConfig, opts ...Option) *Client {
	t.Helper()
	opts = append([]Option{
		WithLogger(slog.New(slog.DiscardHandler)),
		WithDefaults(KindConfig{PollInterval: 10 * time.Millisecond}),
	}, opts...)
	c, err := newClient(st, nil, opts...)
	require.NoError(t, err)
	cfg.Name = "a"
	require.NoError(t, Register(c, h, cfg))
	return c
}

func stopOnCleanup(t *testing.T, c *Client) {
	t.Helper()
	t.Cleanup(func() { stop(t, c) })
}

// stop stops c and requires success. Stop drains handlers including their
// status writes and observer calls, so it is a barrier for asserts.
func stop(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	require.NoError(t, c.Stop(ctx))
}

func TestLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	newC := func(t *testing.T) *Client {
		return newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{})
	}

	t.Run("start twice", func(t *testing.T) {
		t.Parallel()
		c := newC(t)
		stopOnCleanup(t, c)
		require.NoError(t, c.Start(ctx))
		require.ErrorIs(t, c.Start(ctx), ErrAlreadyStarted)
	})
	t.Run("start after stop", func(t *testing.T) {
		t.Parallel()
		c := newC(t)
		stopOnCleanup(t, c)
		require.NoError(t, c.Start(ctx))
		require.NoError(t, c.Stop(ctx))
		require.ErrorIs(t, c.Start(ctx), ErrStopped)
	})
	t.Run("stop before start", func(t *testing.T) {
		t.Parallel()
		c := newC(t)
		require.NoError(t, c.Stop(ctx))
		require.ErrorIs(t, c.Start(ctx), ErrStopped)
	})
	t.Run("stop twice", func(t *testing.T) {
		t.Parallel()
		c := newC(t)
		stopOnCleanup(t, c)
		require.NoError(t, c.Start(ctx))
		require.NoError(t, c.Stop(ctx))
		require.NoError(t, c.Stop(ctx))
	})
	t.Run("concurrent start and stop", func(t *testing.T) {
		t.Parallel()
		c := newC(t)
		stopOnCleanup(t, c)
		var wg sync.WaitGroup
		var startErr, stopErr error
		wg.Go(func() { startErr = c.Start(ctx) })
		wg.Go(func() { stopErr = c.Stop(ctx) })
		wg.Wait()
		require.NoError(t, stopErr)
		if startErr != nil {
			require.ErrorIs(t, startErr, ErrStopped)
		}
		require.NoError(t, c.Stop(ctx))
	})
}

func TestWorkersLimit(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	release := make(chan struct{})
	var running, maxRunning atomic.Int64
	c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
		n := running.Add(1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		<-release
		running.Add(-1)
		return nil
	}, KindConfig{Workers: 2})
	stopOnCleanup(t, c)
	st.feed(5)
	require.NoError(t, c.Start(context.Background()))

	require.Eventually(t, func() bool { return running.Load() == 2 }, waitFor, tick)
	require.Never(t, func() bool { return maxRunning.Load() > 2 }, 200*time.Millisecond, tick)
	require.LessOrEqual(t, st.MaxLimit(), 2)

	close(release)
	require.Eventually(t, func() bool { return len(st.Calls()) == 5 }, waitFor, tick)
	for _, call := range st.Calls() {
		require.Equal(t, StatusDone, call.status)
	}
}

func TestFullBatchRepollsImmediately(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	var executed atomic.Int64
	c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
		executed.Add(1)
		return nil
	}, KindConfig{
		Workers:      3,
		PollInterval: time.Hour,
	})
	stopOnCleanup(t, c)
	st.feed(6)
	require.NoError(t, c.Start(context.Background()))

	require.Eventually(t, func() bool { return executed.Load() == 6 }, waitFor, tick)
	require.Eventually(t, func() bool { return len(st.Calls()) == 6 }, waitFor, tick)
}

func TestPollerSurvivesClaimError(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	st.failClaims(3, errors.New("db down"))
	c := newPoolClient(t, st, noop[testPayload], KindConfig{})
	stopOnCleanup(t, c)
	require.NoError(t, c.Start(context.Background()))

	require.Eventually(t, func() bool { return st.Claims() >= 3 }, waitFor, tick)
	st.feed(1)
	require.Eventually(t, func() bool { return len(st.Calls()) == 1 }, waitFor, tick)
	require.Equal(t, StatusDone, st.Calls()[0].status)
	require.GreaterOrEqual(t, st.Claims(), 4)
}

func TestNoClaimAfterStop(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	c := newPoolClient(t, st, noop[testPayload], KindConfig{})
	stopOnCleanup(t, c)
	require.NoError(t, c.Start(context.Background()))
	require.Eventually(t, func() bool { return st.Claims() >= 2 }, waitFor, tick)

	require.NoError(t, c.Stop(context.Background()))
	// Stop waits for the pollers, so no claim can follow it.
	n := st.Claims()
	require.Equal(t, n, st.Claims())
}

func TestStuckHandlerReported(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	obs := &countObserver{}
	release := make(chan struct{})
	c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
		<-release
		return nil
	}, KindConfig{Timeout: 20 * time.Millisecond},
		WithObserver(obs), WithRescuer(RescuerConfig{Grace: 20 * time.Millisecond}))
	stopOnCleanup(t, c)
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // registered after stopOnCleanup, so it runs first
	st.feed(1)
	require.NoError(t, c.Start(context.Background()))

	require.Eventually(t, func() bool { return obs.stuck.Load() == 1 }, waitFor, tick)
	require.Empty(t, st.Calls())

	unblock()
	require.Eventually(t, func() bool { return len(st.Calls()) == 1 }, waitFor, tick)
	require.Equal(t, StatusDone, st.Calls()[0].status)
	require.EqualValues(t, 1, obs.stuck.Load())
}

func TestClaimInFlightAtStopIsExecuted(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	inClaim := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	st.claimHook = func(ctx context.Context) error {
		first := false
		once.Do(func() { first = true })
		if !first {
			return nil
		}
		close(inClaim)
		<-release
		return ctx.Err()
	}
	c := newPoolClient(t, st, noop[testPayload], KindConfig{})
	stopOnCleanup(t, c)
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	require.NoError(t, c.Start(context.Background()))
	recv(t, inClaim)

	stopErr := make(chan error, 1)
	go func() { stopErr <- c.Stop(context.Background()) }()
	select {
	case <-c.bgCtx.Done():
	case <-time.After(waitFor):
		t.Fatal("Stop did not begin")
	}
	st.feed(1)
	unblock()

	require.NoError(t, recv(t, stopErr))
	calls := st.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, StatusDone, calls[0].status)
}

func TestConcurrentStopWaitsForFirst(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
		started <- struct{}{}
		<-release
		return nil
	}, KindConfig{})
	stopOnCleanup(t, c)
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	st.feed(1)
	require.NoError(t, c.Start(context.Background()))
	recv(t, started)

	first := make(chan error, 1)
	go func() { first <- c.Stop(context.Background()) }()
	select {
	case <-c.bgCtx.Done():
	case <-time.After(waitFor):
		t.Fatal("Stop did not begin")
	}

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.Stop(short), context.DeadlineExceeded)

	second := make(chan error, 1)
	go func() { second <- c.Stop(context.Background()) }()
	require.Never(t, func() bool { return len(second) > 0 }, 100*time.Millisecond, tick)
	require.Empty(t, first)

	unblock()
	require.NoError(t, recv(t, second))
	require.Len(t, st.Calls(), 1)
	require.NoError(t, recv(t, first))
}

// retryableErr is a transient error: pgconn.SafeToRetry reports true for it.
type retryableErr struct{}

func (retryableErr) Error() string     { return "acquire conn" }
func (retryableErr) SafeToRetry() bool { return true }

type thirdPayload struct{}

func (thirdPayload) Kind() Kind { return 4 }

func TestStatusWrite(t *testing.T) {
	t.Parallel()
	t.Run("retries transient", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		st.scriptWrites(writeResult{err: retryableErr{}}, writeResult{err: retryableErr{}})
		obs := &countObserver{}
		var runs atomic.Int64
		c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
			runs.Add(1)
			return nil
		}, KindConfig{}, WithObserver(obs))
		stopOnCleanup(t, c)
		st.feed(1)
		require.NoError(t, c.Start(context.Background()))

		require.Eventually(t, func() bool { return obs.finished.Load() == 1 }, waitFor, tick)
		stop(t, c)
		require.Len(t, st.Calls(), 3)
		require.EqualValues(t, 1, runs.Load())
		require.EqualValues(t, 1, obs.finished.Load())
	})

	t.Run("stops on lost lease", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		st.scriptWrites(writeResult{ok: false})
		obs := &countObserver{}
		var buf syncBuffer
		c := newPoolClient(t, st, noop[testPayload], KindConfig{},
			WithObserver(obs), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		stopOnCleanup(t, c)
		st.feed(1)
		require.NoError(t, c.Start(context.Background()))

		require.Eventually(t, func() bool { return strings.Contains(buf.String(), "job lost lease") }, waitFor, tick)
		stop(t, c)
		require.Contains(t, buf.String(), `level=WARN msg="notasap: job lost lease"`)
		require.Len(t, st.Calls(), 1)
		require.Zero(t, obs.finished.Load())
	})

	t.Run("outcome unknown after retry", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		// The first write timed out but committed, so the retry finds no row.
		st.scriptWrites(writeResult{err: fmt.Errorf("finish: %w", context.DeadlineExceeded)}, writeResult{ok: false})
		obs := &countObserver{}
		var buf syncBuffer
		c := newPoolClient(t, st, noop[testPayload], KindConfig{},
			WithObserver(obs), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		stopOnCleanup(t, c)
		st.feed(1)
		require.NoError(t, c.Start(context.Background()))

		require.Eventually(t, func() bool { return len(st.Calls()) == 2 }, waitFor, tick)
		stop(t, c)
		require.Contains(t, buf.String(), `level=INFO msg="notasap: status write outcome unknown after retry"`)
		require.NotContains(t, buf.String(), "job lost lease")
		require.Zero(t, obs.finished.Load())
	})

	t.Run("lost lease after unsent attempt", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		// The first write was never sent, so ok == false is a lost lease.
		st.scriptWrites(writeResult{err: retryableErr{}}, writeResult{ok: false})
		var buf syncBuffer
		c := newPoolClient(t, st, noop[testPayload], KindConfig{}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		stopOnCleanup(t, c)
		st.feed(1)
		require.NoError(t, c.Start(context.Background()))

		require.Eventually(t, func() bool { return len(st.Calls()) == 2 }, waitFor, tick)
		stop(t, c)
		require.Contains(t, buf.String(), `level=WARN msg="notasap: job lost lease"`)
		require.NotContains(t, buf.String(), "outcome unknown")
	})

	t.Run("no retry on permanent error", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		st.scriptWrites(writeResult{err: &pgconn.PgError{Code: "23505"}})
		var buf syncBuffer
		c := newPoolClient(t, st, noop[testPayload], KindConfig{}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		stopOnCleanup(t, c)
		st.feed(1)
		require.NoError(t, c.Start(context.Background()))

		require.Eventually(t, func() bool { return strings.Contains(buf.String(), "write job status") }, waitFor, tick)
		stop(t, c)
		require.Len(t, st.Calls(), 1)
	})

	t.Run("gives up after budget", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		st.scriptWrites(slices.Repeat([]writeResult{{err: retryableErr{}}}, 50)...)
		obs := &countObserver{}
		var buf syncBuffer
		c := newPoolClient(t, st, noop[testPayload], KindConfig{},
			WithObserver(obs), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		c.dbCallTimeout = 200 * time.Millisecond
		stopOnCleanup(t, c)
		st.feed(1)
		require.NoError(t, c.Start(context.Background()))

		require.Eventually(t, func() bool { return strings.Contains(buf.String(), "write job status") }, waitFor, tick)
		stop(t, c)
		require.Contains(t, buf.String(), `level=WARN msg="notasap: write job status"`)
		// Retried with backoff 50ms, 100ms, ... until the 200ms budget ran out.
		require.Greater(t, len(st.Calls()), 1)
		require.Less(t, len(st.Calls()), 50)
		require.Zero(t, obs.finished.Load())
	})
}

func TestStartPoolWarning(t *testing.T) {
	t.Parallel()
	t.Run("warns on small pool", func(t *testing.T) {
		t.Parallel()
		var buf syncBuffer
		c := newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		require.NoError(t, Register(c, noop[otherPayload], KindConfig{Name: "b"}))
		require.NoError(t, Register(c, noop[thirdPayload], KindConfig{Name: "c"}))
		c.maxConns = 2
		stopOnCleanup(t, c)
		require.NoError(t, c.Start(context.Background()))

		require.Contains(t, buf.String(), "level=WARN")
		require.Contains(t, buf.String(), "max_conns=2")
		require.Contains(t, buf.String(), "MaxConns >= kinds + 2")
		require.NotContains(t, buf.String(), "kinds + 4")
	})

	t.Run("silent when unknown or large", func(t *testing.T) {
		t.Parallel()
		for _, maxConns := range []int32{0, 5} {
			t.Run(fmt.Sprint(maxConns), func(t *testing.T) {
				t.Parallel()
				var buf syncBuffer
				c := newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
				c.maxConns = maxConns
				stopOnCleanup(t, c)
				require.NoError(t, c.Start(context.Background()))
				require.NotContains(t, buf.String(), "level=WARN")
			})
		}
	})
}

func TestStartStopLogged(t *testing.T) {
	t.Parallel()
	var buf syncBuffer
	c := newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	stopOnCleanup(t, c)
	require.NoError(t, c.Start(context.Background()))
	stop(t, c)
	require.Contains(t, buf.String(), `level=INFO msg="notasap: started"`)
	require.Contains(t, buf.String(), `level=INFO msg="notasap: stopped"`)
}

func TestJanitorLoopStartsUnlessDisabled(t *testing.T) {
	t.Parallel()
	// In a synctest bubble, synctest.Wait returns only once every goroutine
	// of the client is durably blocked, so a janitor loop that was started
	// has finished its first pass by then.
	run := func(t *testing.T, opts ...Option) (*fakeStore, *Client) {
		st := newFakeStore()
		c := newPoolClient(t, st, noop[testPayload], KindConfig{}, opts...)
		require.NoError(t, c.Start(context.Background()))
		synctest.Wait()
		return st, c
	}
	t.Run("enabled", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st, c := run(t)
			defer stopInBubble(t, c)
			// One empty batch per status with a positive retention.
			var want int
			for _, status := range janitorStatuses {
				if c.janitor.Retention[status] > 0 {
					want++
				}
			}
			require.Positive(t, want)
			require.Equal(t, want, st.Deletes())
			require.Equal(t, 1, st.Rescues())
		})
	})
	t.Run("disabled", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st, c := run(t, WithJanitor(JanitorConfig{Disabled: true}))
			defer stopInBubble(t, c)
			require.Equal(t, 1, st.Rescues()) // maintenance did start
			require.Zero(t, st.Deletes())
		})
	})
}

// stopInBubble stops c inside its synctest bubble, also after a failed
// assert, so no client goroutine outlives the bubble.
func stopInBubble(t *testing.T, c *Client) {
	t.Helper()
	assert.NoError(t, c.Stop(context.Background()))
}

func TestJanitorPass(t *testing.T) {
	t.Parallel()
	t.Run("stop cancels blocked delete", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		entered := make(chan struct{}, 1)
		st.deleteHook = func(ctx context.Context) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		c := newPoolClient(t, st, noop[testPayload], KindConfig{}, WithJanitor(JanitorConfig{BatchSize: 1}))
		stopOnCleanup(t, c)
		require.NoError(t, c.Start(context.Background()))
		recv(t, entered)

		// Stop returns only if it cancels the blocked delete.
		stopped := make(chan error, 1)
		go func() { stopped <- c.Stop(context.Background()) }()
		require.NoError(t, recv(t, stopped))
		require.Equal(t, 1, st.Deletes())
	})

	t.Run("db error logged at warn", func(t *testing.T) {
		t.Parallel()
		st := newFakeStore()
		st.deleteHook = func(context.Context) error { return errors.New("db down") }
		var buf syncBuffer
		c := newPoolClient(t, st, noop[testPayload], KindConfig{}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
		stopOnCleanup(t, c)
		require.NoError(t, c.Start(context.Background()))
		require.Eventually(t, func() bool { return strings.Contains(buf.String(), "delete finished jobs") }, waitFor, tick)
		stop(t, c)
		require.Contains(t, buf.String(), `level=WARN msg="notasap: delete finished jobs"`)
	})
}

// syncBuffer is a bytes.Buffer safe for concurrent use by loggers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestScheduleWakesPoller(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// run starts a client polling hourly, lets its first poll find nothing,
	// schedules a job and returns how long the job took to run.
	run := func(t *testing.T, opts ...ScheduleOption) time.Duration {
		st := newFakeStore()
		done := make(chan struct{}, 1)
		c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
			done <- struct{}{}
			return nil
		}, KindConfig{PollInterval: time.Hour})
		require.NoError(t, c.Start(ctx))
		defer stopInBubble(t, c)
		synctest.Wait()

		start := time.Now()
		st.feed(1)
		_, err := c.Schedule(ctx, testPayload{}, opts...)
		require.NoError(t, err)
		<-done
		return time.Since(start)
	}
	t.Run("ready job", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			require.Zero(t, run(t))
		})
	})
	t.Run("delayed job", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			require.Equal(t, time.Hour, run(t, WithDelay(time.Minute)))
		})
	})
}

func TestNotify(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	schedule := func(t *testing.T, c *Client) {
		for range 3 {
			_, err := c.Schedule(ctx, testPayload{})
			require.NoError(t, err)
		}
		_, err := c.ScheduleBatch(ctx, Item(testPayload{}), Item(testPayload{}))
		require.NoError(t, err)
	}

	t.Run("never started", func(t *testing.T) {
		t.Parallel()
		c := newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{})
		schedule(t, c)
		require.Empty(t, c.pools)
	})
	t.Run("stopped", func(t *testing.T) {
		t.Parallel()
		c := newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{})
		require.NoError(t, c.Start(ctx))
		stop(t, c)
		schedule(t, c)
		require.Len(t, c.pools[1].notify, 1)
	})
	t.Run("each kind once", func(t *testing.T) {
		t.Parallel()
		c := newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{})
		require.NoError(t, Register(c, noop[otherPayload], KindConfig{Name: "b"}))
		require.NoError(t, c.Start(ctx))
		stopOnCleanup(t, c)
		ps := c.poolsOf([]Kind{1, 2, 1, 99, 2})
		require.Len(t, ps, 2)
		require.Same(t, c.pools[1], ps[0])
		require.Same(t, c.pools[2], ps[1])
	})
}

func TestClaimCoalescing(t *testing.T) {
	t.Parallel()
	const workers = 8 // a poller waits for max(1, workers/4) = 2 free slots

	t.Run("backlog claimed in batches", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			done := make(chan struct{}, 40)
			// Handlers finish one by one, each within 8ms of its start, so while
			// two run another slot frees well within the coalesce window.
			c := newPoolClient(t, st, func(_ context.Context, j *Job[testPayload]) error {
				time.Sleep(time.Duration(j.ID%8+1) * time.Millisecond)
				done <- struct{}{}
				return nil
			}, KindConfig{
				Workers:      workers,
				PollInterval: time.Hour,
			})
			st.feed(40)
			require.NoError(t, c.Start(context.Background()))
			defer stopInBubble(t, c)
			for range 40 {
				<-done
			}

			require.NotContains(t, st.Limits(), 1)
		})
	})

	t.Run("no wait when enough slots free", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			var mu sync.Mutex
			var claimedAt []time.Duration
			start := time.Now()
			st.claimHook = func(context.Context) error {
				mu.Lock()
				defer mu.Unlock()
				claimedAt = append(claimedAt, time.Since(start))
				return nil
			}
			release := make(chan struct{})
			// Jobs 1 and 2 free two slots of the first full batch at once.
			c := newPoolClient(t, st, func(_ context.Context, j *Job[testPayload]) error {
				if j.ID > 2 {
					<-release
				}
				return nil
			}, KindConfig{
				Workers:      workers,
				PollInterval: time.Hour,
			})
			st.feed(16)
			require.NoError(t, c.Start(context.Background()))
			defer stopInBubble(t, c)
			defer close(release)
			synctest.Wait()

			require.Equal(t, []int{workers, 2}, st.Limits())
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []time.Duration{0, 0}, claimedAt)
		})
	})

	t.Run("stop during window", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			release := make(chan struct{})
			// Job 1 frees one slot, so the poller waits in the coalesce window
			// for a second one.
			c := newPoolClient(t, st, func(_ context.Context, j *Job[testPayload]) error {
				if j.ID > 1 {
					<-release
				}
				return nil
			}, KindConfig{
				Workers:      workers,
				PollInterval: time.Hour,
			})
			st.feed(workers + 1)
			require.NoError(t, c.Start(context.Background()))
			synctest.Wait()

			start := time.Now()
			bgDone := make(chan struct{})
			go func() {
				c.bgWG.Wait()
				close(bgDone)
			}()
			stopped := make(chan error, 1)
			go func() { stopped <- c.Stop(context.Background()) }()
			<-bgDone
			require.Zero(t, time.Since(start))
			close(release)
			require.NoError(t, <-stopped)
			require.Equal(t, 1, st.Claims())
		})
	})
}

type poolSuite struct {
	testSuite
}

func TestPool(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(poolSuite))
}

func (s *poolSuite) Test_kindPool_DelayedNotRunEarly() {
	c := s.startClient([]kindReg{reg(noop[payloadA], KindConfig{Name: "a"})})
	delayed := s.schedule(c, payloadA{N: 1}, WithDelay(time.Hour))
	now := s.schedule(c, payloadA{N: 2})

	pgtest.WaitStatus(&s.Suite, now, StatusDone)
	row := s.GetJob(delayed)
	s.Equal(StatusPending, Status(row.Status))
	s.Equal(0, row.Attempt)
}

func (s *poolSuite) Test_kindPool_KindIsolation() {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 1)
	c := s.startClient([]kindReg{
		reg(func(context.Context, *Job[payloadA]) error {
			started <- struct{}{}
			<-release
			return nil
		}, KindConfig{
			Name:    "a",
			Workers: 1,
			Timeout: 10 * time.Minute,
		}),
		reg(noop[payloadB], KindConfig{Name: "b"}),
	}, WithRescuer(RescuerConfig{Grace: 5 * time.Minute}))
	blocked := s.schedule(c, payloadA{})
	recv(s.T(), started)

	// The claim's lease is Timeout + Grace, measured on the DB clock.
	var leaseLeft float64
	s.Require().NoError(s.Pool.QueryRow(s.ctx,
		`SELECT EXTRACT(EPOCH FROM deadline - NOW())::float8 FROM notasap_jobs WHERE id = $1`, blocked).Scan(&leaseLeft))
	s.InDelta((15 * time.Minute).Seconds(), leaseLeft, 2)

	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = s.schedule(c, payloadB{Msg: fmt.Sprint(i)})
	}
	for _, id := range ids {
		pgtest.WaitStatus(&s.Suite, id, StatusDone)
	}
	s.Equal(StatusRunning, Status(s.GetJob(blocked).Status))
}

func (s *poolSuite) Test_kindPool_TwoClientsNoDoubleExecution() {
	const n = 200
	var seen sync.Map
	// Each client's first handler call waits until both clients have run
	// one, so neither can drain the queue alone: Workers bounds the claim.
	var ready sync.WaitGroup
	ready.Add(2)
	bothReady := make(chan struct{})
	go func() { ready.Wait(); close(bothReady) }()
	handler := func(processed *atomic.Int64) Handler[payloadA] {
		arrived := sync.OnceFunc(ready.Done)
		return func(ctx context.Context, j *Job[payloadA]) error {
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
	cfg := KindConfig{
		Name:    "a",
		Workers: 2,
	}
	c1 := s.startClient([]kindReg{reg(handler(&processed1), cfg)})
	s.startClientOn(pgtest.NewPool(s.T(), s.Schema), []kindReg{reg(handler(&processed2), cfg)})

	items := make([]BatchItem, n)
	for i := range items {
		items[i] = Item(payloadA{N: i})
	}
	_, err := c1.ScheduleBatch(s.ctx, items...)
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

func (s *poolSuite) Test_kindPool_ScheduleWakesIdlePoller() {
	c := s.startClient([]kindReg{reg(noop[payloadA], KindConfig{
		Name:         "a",
		PollInterval: time.Hour,
	})})
	// The first poll finds nothing, so the poller sleeps for PollInterval.
	s.Eventually(func() bool { return c.obs.Count("polled/a") >= 1 }, waitFor, tick)
	id := s.schedule(c, payloadA{})

	pgtest.WaitStatus(&s.Suite, id, StatusDone)
	s.Equal(2, c.obs.Count("polled/a"))
}

func (s *poolSuite) Test_kindPool_LockedRowDoesNotStallWrites() {
	release := make(chan struct{})
	c := s.startClient([]kindReg{reg(func(ctx context.Context, _ *Job[payloadA]) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}, KindConfig{
		Name:    "a",
		Workers: 3,
	})})
	ids := []int64{s.schedule(c, payloadA{}), s.schedule(c, payloadA{}), s.schedule(c, payloadA{})}
	s.WaitFor(func() bool { return s.CountJobs(store.StatusRunning) == 3 }, "jobs not running")

	tx, err := s.Pool.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback(s.ctx) }()
	_, err = tx.Exec(s.ctx, "SELECT id FROM notasap_jobs WHERE id = $1 FOR UPDATE", ids[0])
	s.Require().NoError(err)
	close(release)

	// The other rows are written while the locked one waits for tx.
	pgtest.WaitStatus(&s.Suite, ids[1], StatusDone)
	pgtest.WaitStatus(&s.Suite, ids[2], StatusDone)
	s.WaitBlockedBy(s.BackendPID(tx))
	s.Equal(StatusRunning, Status(s.GetJob(ids[0]).Status))

	s.Require().NoError(tx.Commit(s.ctx))
	pgtest.WaitStatus(&s.Suite, ids[0], StatusDone)
	s.Equal(3, c.obs.Count("finished/done"))
}
