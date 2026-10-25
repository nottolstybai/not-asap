package notasap

import (
	"context"
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
	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/internal/store"
)

// startCompleter returns a running completer of a client over st and a
// function that queues n done results with ids from first.
func startCompleter(t *testing.T, st *fakeStore, opts ...Option) (*completer, func(first, n int)) {
	t.Helper()
	c := newPoolClient(t, st, noop[testPayload], KindConfig{}, opts...)
	w := newCompleter(c)
	go w.loop()
	add := func(first, n int) {
		for i := range n {
			w.add(completion{
				ref: store.Ref{
					ID:    int64(first + i),
					Lease: 1,
				},
				kind:   "a",
				a:      action{status: StatusDone},
				logger: c.logger,
			})
		}
	}

	return w, add
}

// blockFirstWrite makes the first Complete call of st wait for release; the
// returned channel receives once it is in flight.
func blockFirstWrite(st *fakeStore, release <-chan struct{}) <-chan struct{} {
	entered := make(chan struct{})
	var started atomic.Bool
	st.completeHook = func(context.Context) {
		if started.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}

	return entered
}

func TestCompleter(t *testing.T) {
	t.Parallel()
	t.Run("coalesces", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			release := make(chan struct{})
			entered := blockFirstWrite(st, release)
			w, add := startCompleter(t, st)
			add(1, 1)
			<-entered
			add(2, 4)

			close(release)
			w.close()
			<-w.done
			require.Equal(t, []int{1, 4}, st.Batches())
		})
	})

	t.Run("caps batch", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			release := make(chan struct{})
			entered := blockFirstWrite(st, release)
			w, add := startCompleter(t, st)
			add(1, 1)
			<-entered
			add(2, 2500)

			close(release)
			w.close()
			<-w.done
			require.Equal(t, []int{1, maxCompleteBatch, maxCompleteBatch, 500}, st.Batches())
		})
	})

	t.Run("retries transient", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			release := make(chan struct{})
			entered := blockFirstWrite(st, release)
			st.scriptWrites(writeResult{ok: true}, writeResult{err: retryableErr{}})
			obs := &countObserver{}
			w, add := startCompleter(t, st, WithObserver(obs))
			add(1, 1)
			<-entered
			add(2, 3)

			start := time.Now()
			close(release)
			w.close()
			<-w.done
			require.Equal(t, []int{1, 3, 3}, st.Batches())
			require.Equal(t, statusWriteBackoff, time.Since(start))
			require.EqualValues(t, 4, obs.finished.Load())
		})
	})

	t.Run("outcome unknown", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			// The first write timed out but committed, so the retry finds no row.
			st.scriptWrites(writeResult{err: fmt.Errorf("complete: %w", context.DeadlineExceeded)}, writeResult{ok: false})
			obs := &countObserver{}
			var buf syncBuffer
			c := newPoolClient(t, st, noop[testPayload], KindConfig{},
				WithObserver(obs), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
			w := newCompleter(c)
			for id := range int64(2) {
				w.add(completion{
					ref: store.Ref{
						ID:    id + 1,
						Lease: 1,
					},
					kind:   "a",
					a:      action{status: StatusDone},
					logger: c.logger,
				})
			}

			go w.loop()
			w.close()
			<-w.done
			require.Equal(t, []int{2, 2}, st.Batches())
			require.Equal(t, 2, strings.Count(buf.String(), `level=INFO msg="notasap: status write outcome unknown after retry"`))
			require.NotContains(t, buf.String(), "job lost lease")
			require.Zero(t, obs.finished.Load())
		})
	})

	t.Run("close unblocks add", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			release := make(chan struct{})
			entered := blockFirstWrite(st, release)
			w, add := startCompleter(t, st)
			add(1, 1)
			<-entered
			add(2, maxQueued)
			added := make(chan struct{})
			go func() {
				add(maxQueued+2, 1)
				close(added)
			}()
			synctest.Wait()
			require.Empty(t, added)

			w.close()
			<-added // written directly
			close(release)
			<-w.done
			require.Equal(t, []int{1, 1, maxCompleteBatch, maxCompleteBatch, maxCompleteBatch, maxCompleteBatch}, st.Batches())
		})
	})

	t.Run("direct after close", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			w, add := startCompleter(t, st)
			w.close()
			<-w.done

			add(1, 2)
			// No loop is left, so each result is its own write.
			require.Equal(t, []int{1, 1}, st.Batches())
		})
	})
}

// logClient returns a client over st that logs to buf at Debug and a
// completer of it; results are queued with the client's logger.
func logClient(t *testing.T, st *fakeStore, buf *syncBuffer, opts ...Option) (*Client, *completer) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c := newPoolClient(t, st, noop[testPayload], KindConfig{}, append(opts, WithLogger(logger))...)

	return c, newCompleter(c)
}

// queue adds a done result for each id to w without starting its loop, so
// they are written as one batch.
func queue(c *Client, w *completer, ids ...int64) {
	for _, id := range ids {
		w.add(completion{
			ref: store.Ref{
				ID:    id,
				Lease: 1,
			},
			kind:   "a",
			a:      action{status: StatusDone},
			logger: c.logger.With("job_id", id),
		})
	}
}

func TestCompleterRows(t *testing.T) {
	t.Parallel()
	t.Run("poison row", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			poison := &pgconn.PgError{Code: "22021"}
			st.scriptWrites(writeResult{err: poison}, writeResult{ok: true}, writeResult{err: poison}, writeResult{ok: true})
			obs := &countObserver{}
			var buf syncBuffer
			c, w := logClient(t, st, &buf, WithObserver(obs))
			queue(c, w, 1, 2, 3)

			go w.loop()
			w.close()
			<-w.done
			require.Equal(t, []int{3, 1, 1, 1}, st.Batches())
			require.EqualValues(t, 2, obs.finished.Load())
			require.Equal(t, 1, strings.Count(buf.String(), `level=WARN msg="notasap: write job statuses, writing them one by one" jobs=3`))
			require.Equal(t, 1, strings.Count(buf.String(), `level=WARN msg="notasap: write job status" job_id=2`))
			require.Equal(t, 1, strings.Count(buf.String(), "level=WARN msg=\"notasap: write job status\""))
		})
	})

	t.Run("slow path", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			st.scriptWrites(writeResult{
				ok:      true,
				missing: []int64{2, 3},
			})
			st.scriptOnes(writeResult{ok: true})
			obs := &countObserver{}
			var buf syncBuffer
			c, w := logClient(t, st, &buf, WithObserver(obs))
			queue(c, w, 1, 2, 3)

			go w.loop()
			w.close()
			<-w.done
			require.Equal(t, []int{3}, st.Batches())
			require.Equal(t, []store.Ref{{ID: 2, Lease: 1}, {ID: 3, Lease: 1}}, st.Ones())
			require.EqualValues(t, 2, obs.finished.Load())
			require.Equal(t, 1, strings.Count(buf.String(), "job lost lease"))
			require.Contains(t, buf.String(), `level=WARN msg="notasap: job lost lease" job_id=3`)
		})
	})

	t.Run("slow path after timeout", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			st.scriptWrites(writeResult{err: fmt.Errorf("complete: %w", context.DeadlineExceeded)}, writeResult{
				ok:      true,
				missing: []int64{2},
			})
			obs := &countObserver{}
			var buf syncBuffer
			c, w := logClient(t, st, &buf, WithObserver(obs))
			queue(c, w, 1, 2)

			go w.loop()
			w.close()
			<-w.done
			require.Equal(t, []store.Ref{{ID: 2, Lease: 1}}, st.Ones())
			require.EqualValues(t, 1, obs.finished.Load())
			require.Contains(t, buf.String(), `level=INFO msg="notasap: status write outcome unknown after retry" job_id=2`)
			require.NotContains(t, buf.String(), "job lost lease")
		})
	})

	t.Run("failed batch log", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			st := newFakeStore()
			st.scriptWrites(slices.Repeat([]writeResult{{err: retryableErr{}}}, 50)...)
			var buf syncBuffer
			c, w := logClient(t, st, &buf)
			queue(c, w, 1, 2, 3)

			go w.loop()
			w.close()
			<-w.done
			require.Equal(t, 1, strings.Count(buf.String(), "level=WARN"))
			require.Contains(t, buf.String(), `level=WARN msg="notasap: write job statuses" jobs=3`)
			require.Equal(t, 3, strings.Count(buf.String(), `level=DEBUG msg="notasap: write job status"`))
			require.Empty(t, st.Ones())
		})
	})
}

func TestStopWaitsForQueuedResults(t *testing.T) {
	t.Parallel()
	st := newFakeStore()
	release := make(chan struct{})
	entered := blockFirstWrite(st, release)
	obs := &countObserver{}
	c := newPoolClient(t, st, noop[testPayload], KindConfig{Workers: 5}, WithObserver(obs))
	stopOnCleanup(t, c)
	st.feed(5)
	require.NoError(t, c.Start(context.Background()))
	recv(t, entered)

	// Slots are freed once results are handed off, before they are written.
	require.Eventually(t, func() bool { return obs.ended.Load() == 5 }, waitFor, tick)
	stopErr := make(chan error, 1)
	go func() { stopErr <- c.Stop(context.Background()) }()
	require.Never(t, func() bool { return len(stopErr) > 0 }, 100*time.Millisecond, tick)
	require.Zero(t, obs.finished.Load())

	close(release)
	require.NoError(t, recv(t, stopErr))
	require.Len(t, st.Calls(), 5)
	require.EqualValues(t, 5, obs.finished.Load())
	n := len(st.Batches())
	require.Never(t, func() bool { return len(st.Batches()) > n }, 50*time.Millisecond, tick)
}

func TestFullQueueKeepsSlot(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := newFakeStore()
		release := make(chan struct{})
		entered := blockFirstWrite(st, release)
		obs := &countObserver{}
		c := newPoolClient(t, st, noop[testPayload], KindConfig{Workers: 1}, WithObserver(obs))
		require.NoError(t, c.Start(context.Background()))
		defer stopInBubble(t, c)
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock() // runs before stopInBubble
		queue := func(id int64) {
			c.completer.add(completion{
				ref: store.Ref{
					ID:    id,
					Lease: 1,
				},
				kind:   "a",
				a:      action{status: StatusDone},
				logger: c.logger,
			})
		}
		// One result in the blocked write, maxQueued behind it.
		queue(-1)
		<-entered
		for id := range int64(maxQueued) {
			queue(-2 - id)
		}

		st.feed(1)
		time.Sleep(time.Second) // past PollInterval: the job is claimed and run
		synctest.Wait()
		require.Zero(t, obs.ended.Load())
		require.Len(t, c.pools[1].slots, 1)

		unblock()
		synctest.Wait()
		require.EqualValues(t, 1, obs.ended.Load())
		require.Empty(t, c.pools[1].slots)
		require.Len(t, st.Calls(), maxQueued+2)
	})
}

func TestStopWritesWithinGrace(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := newFakeStore()
		st.completeHook = func(context.Context) { time.Sleep(time.Second) }
		h := func(ctx context.Context, _ *Job[testPayload]) error {
			<-ctx.Done()
			return ctx.Err()
		}
		c := newPoolClient(t, st, h, KindConfig{Workers: 3}, WithStopGrace(5*time.Second))
		st.feed(3)
		require.NoError(t, c.Start(context.Background()))
		time.Sleep(time.Second) // past PollInterval: the jobs are claimed and run
		synctest.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.ErrorIs(t, c.Stop(ctx), context.DeadlineExceeded)
		calls := st.Calls()
		require.Len(t, calls, 3)
		for _, call := range calls {
			require.Equal(t, StatusPending, call.status)
			require.True(t, call.refund)
		}
	})
}
