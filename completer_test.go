package notasap

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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
	t.Run("coalesces results during a write", func(t *testing.T) {
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

	t.Run("caps a batch", func(t *testing.T) {
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

	t.Run("retries transient as a batch", func(t *testing.T) {
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

	t.Run("outcome unknown for the whole batch", func(t *testing.T) {
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

	t.Run("close unblocks a waiting add", func(t *testing.T) {
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

	t.Run("writes directly after close", func(t *testing.T) {
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
