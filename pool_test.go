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
)

const (
	waitFor = 5 * time.Second
	tick    = 5 * time.Millisecond
)

// countObserver counts JobStuck and JobFinished calls.
type countObserver struct {
	NopObserver
	stuck, finished atomic.Int64
}

func (o *countObserver) JobStuck(string)                           { o.stuck.Add(1) }
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
	}, KindConfig{Workers: 3, PollInterval: time.Hour})
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
			}, KindConfig{Workers: workers, PollInterval: time.Hour})
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
			}, KindConfig{Workers: workers, PollInterval: time.Hour})
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
			}, KindConfig{Workers: workers, PollInterval: time.Hour})
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
