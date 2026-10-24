package notasap

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stuckObserver counts JobStuck calls.
type stuckObserver struct {
	NopObserver
	stuck atomic.Int64
}

func (o *stuckObserver) JobStuck(string) { o.stuck.Add(1) }

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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, c.Stop(ctx))
	})
}

func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	newC := func(t *testing.T) *Client {
		return newPoolClient(t, newFakeStore(), noopHandler[testPayload](), KindConfig{})
	}

	t.Run("start twice", func(t *testing.T) {
		c := newC(t)
		stopOnCleanup(t, c)
		require.NoError(t, c.Start(ctx))
		require.ErrorIs(t, c.Start(ctx), ErrAlreadyStarted)
	})
	t.Run("start after stop", func(t *testing.T) {
		c := newC(t)
		require.NoError(t, c.Start(ctx))
		require.NoError(t, c.Stop(ctx))
		require.ErrorIs(t, c.Start(ctx), ErrStopped)
	})
	t.Run("stop before start", func(t *testing.T) {
		c := newC(t)
		require.NoError(t, c.Stop(ctx))
		require.ErrorIs(t, c.Start(ctx), ErrStopped)
	})
	t.Run("stop twice", func(t *testing.T) {
		c := newC(t)
		require.NoError(t, c.Start(ctx))
		require.NoError(t, c.Stop(ctx))
		require.NoError(t, c.Stop(ctx))
	})
	t.Run("concurrent start and stop", func(t *testing.T) {
		c := newC(t)
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

	require.Eventually(t, func() bool { return running.Load() == 2 }, 5*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return maxRunning.Load() > 2 }, 200*time.Millisecond, 5*time.Millisecond)
	require.LessOrEqual(t, st.MaxLimit(), 2)

	close(release)
	require.Eventually(t, func() bool { return len(st.Calls()) == 5 }, 5*time.Second, 5*time.Millisecond)
	for _, call := range st.Calls() {
		require.Equal(t, StatusDone, call.status)
	}
}

func TestFullBatchRepollsImmediately(t *testing.T) {
	st := newFakeStore()
	var executed atomic.Int64
	c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
		executed.Add(1)
		return nil
	}, KindConfig{Workers: 3, PollInterval: time.Hour})
	stopOnCleanup(t, c)
	st.feed(6)
	require.NoError(t, c.Start(context.Background()))

	require.Eventually(t, func() bool { return executed.Load() == 6 }, 5*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(st.Calls()) == 6 }, 5*time.Second, 5*time.Millisecond)
}

func TestPollerSurvivesClaimError(t *testing.T) {
	st := newFakeStore()
	st.failClaims(3, errors.New("db down"))
	c := newPoolClient(t, st, noopHandler[testPayload](), KindConfig{})
	stopOnCleanup(t, c)
	require.NoError(t, c.Start(context.Background()))

	require.Eventually(t, func() bool { return st.Claims() >= 3 }, 5*time.Second, 5*time.Millisecond)
	st.feed(1)
	require.Eventually(t, func() bool { return len(st.Calls()) == 1 }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, StatusDone, st.Calls()[0].status)
	require.GreaterOrEqual(t, st.Claims(), 4)
}

func TestNoClaimAfterStop(t *testing.T) {
	st := newFakeStore()
	c := newPoolClient(t, st, noopHandler[testPayload](), KindConfig{})
	require.NoError(t, c.Start(context.Background()))
	require.Eventually(t, func() bool { return st.Claims() >= 2 }, 5*time.Second, 5*time.Millisecond)

	require.NoError(t, c.Stop(context.Background()))
	n := st.Claims()
	require.Never(t, func() bool { return st.Claims() != n }, 100*time.Millisecond, 5*time.Millisecond)
}

func TestStuckHandlerReported(t *testing.T) {
	st := newFakeStore()
	obs := &stuckObserver{}
	release := make(chan struct{})
	c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
		<-release
		return nil
	}, KindConfig{Timeout: 20 * time.Millisecond},
		WithObserver(obs), WithRescuer(RescuerConfig{Grace: 20 * time.Millisecond}))
	stopOnCleanup(t, c)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	st.feed(1)
	require.NoError(t, c.Start(context.Background()))

	require.Eventually(t, func() bool { return obs.stuck.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
	require.Empty(t, st.Calls())

	once.Do(func() { close(release) })
	require.Eventually(t, func() bool { return len(st.Calls()) == 1 }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, StatusDone, st.Calls()[0].status)
	require.EqualValues(t, 1, obs.stuck.Load())
}

func TestClaimInFlightAtStopIsExecuted(t *testing.T) {
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
	c := newPoolClient(t, st, noopHandler[testPayload](), KindConfig{})
	require.NoError(t, c.Start(context.Background()))
	<-inClaim

	stopErr := make(chan error, 1)
	go func() { stopErr <- c.Stop(context.Background()) }()
	select {
	case <-c.bgCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not begin")
	}
	st.feed(1)
	close(release)

	select {
	case err := <-stopErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	calls := st.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, StatusDone, calls[0].status)
}

func TestConcurrentStopWaitsForFirst(t *testing.T) {
	st := newFakeStore()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	c := newPoolClient(t, st, func(context.Context, *Job[testPayload]) error {
		started <- struct{}{}
		<-release
		return nil
	}, KindConfig{})
	st.feed(1)
	require.NoError(t, c.Start(context.Background()))
	<-started

	first := make(chan error, 1)
	go func() { first <- c.Stop(context.Background()) }()
	select {
	case <-c.bgCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not begin")
	}

	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.Stop(short), context.DeadlineExceeded)

	second := make(chan error, 1)
	go func() { second <- c.Stop(context.Background()) }()
	require.Never(t, func() bool { return len(second) > 0 }, 100*time.Millisecond, 5*time.Millisecond)
	require.Empty(t, first)

	close(release)
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second Stop did not return")
	}
	require.Len(t, st.Calls(), 1)
	require.NoError(t, <-first)
}
