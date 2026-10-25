package notasap

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/nottolstybai/not-asap/internal/store"
)

// maxCompleteBatch caps the rows of one status write.
const maxCompleteBatch = 1000

// maxQueued caps the results waiting for a write: enough for a few batches
// behind the one in flight, so a slow database blocks handlers in add (they
// keep their slots, and claiming slows down) instead of queueing without
// bound.
const maxQueued = 4 * maxCompleteBatch

// completion is a handler's result waiting to be written, with what is
// needed to report it.
type completion struct {
	ref    store.Ref
	kind   string
	a      action
	took   time.Duration
	logger *slog.Logger
	// unknown is set once a write attempt timed out: it may have committed,
	// so a row missing from a later attempt does not prove a lost lease.
	unknown bool
}

func (r completion) param() store.Completion {
	return store.Completion{
		Ref:    r.ref,
		Status: int16(r.a.status),
		Delay:  r.a.delay,
		Error:  r.a.errMsg,
		Refund: r.a.refund,
	}
}

// completer writes job results in batches (group commit): a result arriving
// while no write is in flight is written at once; results arriving during a
// write go together into the next one. A batch skips rows locked by another
// transaction; those, and rows whose lease looks lost, go to the slow path,
// which writes them one at a time, waiting for locks, in its own goroutine,
// so the batch writer never waits for another transaction.
type completer struct {
	c        *Client
	kick     chan struct{} // signalled when queue grows or on close; cap 1
	slowKick chan struct{} // signalled when slow grows or loop ends; cap 1
	done     chan struct{} // closed when loop and the slow path return

	mu sync.Mutex
	// space is signalled when the queue shrinks or on close.
	space  *sync.Cond
	queue  []completion
	closed bool
	// slow holds the results the batch writer did not write.
	slow []completion
	// batchDone is set when loop is about to return: slow gets no more
	// results.
	batchDone bool
}

func newCompleter(c *Client) *completer {
	w := &completer{
		c:        c,
		kick:     make(chan struct{}, 1),
		slowKick: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	w.space = sync.NewCond(&w.mu)

	return w
}

// add queues r for writing, waiting while maxQueued results are queued; the
// wait ends with the write in flight, which is bounded by dbCallTimeout.
// After close it writes r itself, so a handler that outlives Stop still
// saves its result.
func (w *completer) add(r completion) {
	w.mu.Lock()
	for len(w.queue) >= maxQueued && !w.closed {
		w.space.Wait()
	}

	if w.closed {
		w.mu.Unlock()
		for _, s := range w.write([]completion{r}) {
			w.writeSlow(s)
		}

		return
	}

	w.queue = append(w.queue, r)
	w.mu.Unlock()
	signal(w.kick)
}

// close makes loop return once the queue is written and the slow path once
// it is written too; later results are written by add directly.
func (w *completer) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.space.Broadcast()
	signal(w.kick)
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// loop writes queued results until close and an empty queue, then waits for
// the slow path.
func (w *completer) loop() {
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		w.slowLoop()
	}()

	for {
		batch, closed := w.take()
		if len(batch) > 0 {
			w.toSlow(w.write(batch))
			continue
		}

		if closed {
			break
		}

		<-w.kick
	}

	w.mu.Lock()
	w.batchDone = true
	w.mu.Unlock()
	signal(w.slowKick)
	<-slowDone
	close(w.done)
}

// take removes up to maxCompleteBatch results from the queue.
func (w *completer) take() ([]completion, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := min(len(w.queue), maxCompleteBatch)
	batch := w.queue[:n:n]
	w.queue = w.queue[n:]
	if len(w.queue) == 0 {
		w.queue = nil
	}

	if n > 0 {
		w.space.Broadcast()
	}

	return batch, w.closed
}

// toSlow hands rs to the slow path.
func (w *completer) toSlow(rs []completion) {
	if len(rs) == 0 {
		return
	}

	w.mu.Lock()
	w.slow = append(w.slow, rs...)
	w.mu.Unlock()
	signal(w.slowKick)
}

// slowLoop writes the results handed to the slow path, one at a time, until
// loop has ended and none is left.
func (w *completer) slowLoop() {
	for {
		w.mu.Lock()
		if len(w.slow) == 0 {
			last := w.batchDone
			w.slow = nil
			w.mu.Unlock()
			if last {
				return
			}

			<-w.slowKick
			continue
		}

		r := w.slow[0]
		w.slow = w.slow[1:]
		w.mu.Unlock()
		w.writeSlow(r)
	}
}

// write saves batch in one statement and reports the jobs written; it returns
// the results it did not write, for the slow path. Transient failures are
// retried with doubling backoff, all within one dbCallTimeout. After any other
// failure the rows are written one by one, so one bad row does not fail the
// others.
func (w *completer) write(batch []completion) []completion {
	c := w.c
	ctx, cancel := context.WithTimeout(context.Background(), c.dbCallTimeout)
	defer cancel()

	params := make([]store.Completion, len(batch))
	for i, r := range batch {
		params[i] = r.param()
	}

	var written []store.Ref
	unknown, err := retry(ctx, func() error {
		var err error
		written, err = c.st.Complete(ctx, params)
		return err
	})
	switch {
	case err == nil:
		return w.settle(batch, written, unknown)
	case len(batch) == 1:
		batch[0].logger.Warn("notasap: write job status", "status", batch[0].a.status, "error", err)
		return nil
	case !isTransient(err):
		c.logger.Warn("notasap: write job statuses, writing them one by one", "jobs", len(batch), "error", err)
		return w.writeRows(ctx, batch, params, unknown)
	}

	c.logger.Warn("notasap: write job statuses", "jobs", len(batch), "error", err)
	for _, r := range batch {
		r.logger.Debug("notasap: write job status", "status", r.a.status, "error", err)
	}

	return nil
}

// settle reports the jobs of batch found in written and returns the others.
func (w *completer) settle(batch []completion, written []store.Ref, unknown bool) []completion {
	ok := make(map[store.Ref]struct{}, len(written))
	for _, ref := range written {
		ok[ref] = struct{}{}
	}

	var rest []completion
	for _, r := range batch {
		if _, found := ok[r.ref]; found {
			w.c.report(r, true, false)
			continue
		}

		r.unknown = r.unknown || unknown
		rest = append(rest, r)
	}

	return rest
}

// writeRows writes batch one row per statement and returns the results it did
// not write, for the slow path; failed rows are logged.
func (w *completer) writeRows(ctx context.Context, batch []completion, params []store.Completion, unknown bool) []completion {
	var rest []completion
	for i, r := range batch {
		var written []store.Ref
		rowUnknown, err := retry(ctx, func() error {
			var err error
			written, err = w.c.st.Complete(ctx, params[i:i+1])
			return err
		})
		if err != nil {
			r.logger.Warn("notasap: write job status", "status", r.a.status, "error", err)
			continue
		}

		rest = append(rest, w.settle([]completion{r}, written, unknown || rowUnknown)...)
	}

	return rest
}

// writeSlow writes r waiting for its row lock and reports it; no row written
// means a lost lease, or an unknown outcome if an attempt timed out.
func (w *completer) writeSlow(r completion) {
	c := w.c
	ctx, cancel := context.WithTimeout(context.Background(), c.dbCallTimeout)
	defer cancel()

	var written bool
	unknown, err := retry(ctx, func() error {
		var err error
		written, err = c.st.CompleteOne(ctx, r.param())
		return err
	})
	if err != nil {
		r.logger.Warn("notasap: write job status", "status", r.a.status, "error", err)
		return
	}

	c.report(r, written, r.unknown || unknown)
}

// retry calls f until it succeeds, fails with an error that is not
// transient, or ctx is done, waiting statusWriteBackoff, doubling, between
// attempts. It reports whether an attempt timed out before a retry: it may
// have committed.
func retry(ctx context.Context, f func() error) (bool, error) {
	unknown := false
	err := f()
	for delay := statusWriteBackoff; err != nil && isTransient(err); delay *= 2 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}

		if ctx.Err() != nil {
			break
		}

		unknown = unknown || timedOut(err)
		err = f()
	}

	return unknown, err
}

// report logs the outcome of a written attempt to save r and tells the
// observer.
func (c *Client) report(r completion, written, unknown bool) {
	a, logger := r.a, r.logger
	switch {
	case !written && unknown:
		// The retry cannot tell a committed earlier attempt from a lost
		// lease. The observer is not called, as for a lost lease.
		logger.Info("notasap: status write outcome unknown after retry", "status", a.status)
		return
	case !written:
		logger.Warn("notasap: job lost lease", "status", a.status)
		return
	}

	switch a.status {
	case StatusPending:
		c.observer.JobRetried(r.kind, a.reason, r.took)
		logger.Info("notasap: job retried", "reason", a.reason, "delay", a.delay, "error", a.errMsg)
	case StatusDone:
		c.observer.JobFinished(r.kind, a.status, r.took)
		logger.Debug("notasap: job done", "took", r.took)
	case StatusCancelled:
		c.observer.JobFinished(r.kind, a.status, r.took)
		logger.Info("notasap: job cancelled", "error", a.errMsg)
	default:
		c.observer.JobFinished(r.kind, a.status, r.took)
		logger.Error("notasap: job failed", "reason", a.reason, "error", a.errMsg)
	}
}
