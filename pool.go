package notasap

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nottolstybai/not-asap/internal/store"
)

// dbCallTimeout bounds claiming jobs and writing a job's status after its
// handler returns, retries included. Neither inherits the handler or shutdown
// contexts. It is a variable only so tests can shorten it.
var dbCallTimeout = 10 * time.Second

// statusWriteBackoff is the delay before the first retry of a status write; it
// doubles after each attempt.
const statusWriteBackoff = 50 * time.Millisecond

// claimCoalesceWindow bounds how long a poller under backlog waits for more
// slots to free before it claims.
const claimCoalesceWindow = 20 * time.Millisecond

// isTransient reports whether err is a connection-level failure (pool acquire
// or timeout) after which retrying a status write is safe: the lease makes the
// write idempotent.
func isTransient(err error) bool {
	return pgconn.SafeToRetry(err) || pgconn.Timeout(err)
}

// kindPool polls and runs the jobs of one Kind with at most Workers handlers.
type kindPool struct {
	c     *Client
	entry *kindEntry
	slots chan struct{} // one token per running handler; cap is Workers
	wake  chan struct{} // signalled when a slot frees; cap 1
	// notify is signalled when this client schedules a ready job of the
	// kind; cap 1.
	notify chan struct{}
}

// loop claims jobs while there are free slots until the client stops. After a
// full batch it claims again once slots free, waiting at most
// claimCoalesceWindow (20ms) for max(1, Workers/4) of them; after a short
// batch or a claim error it waits PollInterval or until the client schedules
// a ready job of the kind.
func (p *kindPool) loop() {
	c, cfg := p.c, p.entry.cfg
	lease := cfg.Timeout + c.rescuer.Grace
	timer := time.NewTimer(cfg.PollInterval)
	timer.Stop()
	defer timer.Stop()

	backlog := false
	for {
		free, ok := p.waitFree(backlog)
		if !ok {
			return
		}
		// The claim does not use bgCtx: cancelling it mid-query could commit
		// the claim while losing its rows. Claimed rows are always dispatched
		// and then drained by Stop.
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), dbCallTimeout)
		rows, err := c.st.Claim(ctx, int16(p.entry.kind), free, lease)
		cancel()
		c.observer.Polled(cfg.Name, time.Since(start))
		if err != nil {
			select {
			case <-c.stopCh:
				return
			default:
			}
			c.logger.Warn("notasap: claim jobs", "kind", cfg.Name, "error", err)
		}
		for _, row := range rows {
			p.slots <- struct{}{}
			c.jobsWG.Go(func() { p.execute(row) })
		}
		backlog = err == nil && len(rows) == free
		if !backlog {
			if !p.sleep(timer, cfg.PollInterval) {
				return
			}
		}
	}
}

// waitFree returns the number of free slots once there is one. If all slots
// were busy, or backlog says the last claim was a full batch, it then waits up
// to claimCoalesceWindow for a quarter of them to be free, so a backlog is
// claimed in batches rather than one row at a time. It returns false once the
// client stops.
func (p *kindPool) waitFree(backlog bool) (int, bool) {
	stopCh := p.c.stopCh
	free := cap(p.slots) - len(p.slots)
	if free == 0 || backlog {
		for free == 0 {
			select {
			case <-p.wake:
			case <-stopCh:
				return 0, false
			}
			free = cap(p.slots) - len(p.slots)
		}
		if want := max(1, cap(p.slots)/4); free < want {
			window := time.NewTimer(claimCoalesceWindow)
			defer window.Stop()
		coalesce:
			for free < want {
				select {
				case <-p.wake:
				case <-window.C:
					break coalesce
				case <-stopCh:
					return 0, false
				}
				free = cap(p.slots) - len(p.slots)
			}
		}
	}
	select {
	case <-stopCh:
		return 0, false
	default:
	}
	return free, true
}

// sleep waits d on timer or until notify is signalled. It returns false once
// the client stops.
func (p *kindPool) sleep(timer *time.Timer, d time.Duration) bool {
	timer.Reset(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-p.notify:
		return true
	case <-p.c.stopCh:
		return false
	}
}

// execute runs the handler for a claimed job, waits for it even past its
// timeout, records the outcome and frees the slot.
func (p *kindPool) execute(row store.ClaimedJob) {
	c, cfg := p.c, p.entry.cfg
	defer func() {
		c.observer.JobEnded(cfg.Name)
		<-p.slots
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}()

	logger := c.logger.With("job_id", row.ID, "kind", cfg.Name, "attempt", row.Attempt)
	ctx, cancel := context.WithTimeoutCause(c.runCtx, cfg.Timeout, errTimeout)
	defer cancel()
	ctx = withLogger(ctx, logger)

	c.observer.JobStarted(cfg.Name)
	start := time.Now()
	stuck := time.AfterFunc(cfg.Timeout+c.rescuer.Grace, func() {
		logger.Error("notasap: handler ignores context", "timeout", cfg.Timeout)
		c.observer.JobStuck(cfg.Name)
	})
	res := p.run(ctx, row, logger)
	stuck.Stop()
	res.cause = context.Cause(ctx)
	took := time.Since(start)

	a := decide(res, row.Attempt, row.MaxAttempts, cfg.Backoff, logger)
	p.record(row, a, took, logger)
}

// run calls the handler, turning a panic into a failed result.
func (p *kindPool) run(ctx context.Context, row store.ClaimedJob, logger *slog.Logger) (res execResult) {
	defer func() {
		if v := recover(); v != nil {
			logger.Error("notasap: handler panicked", "panic", v, "stack", string(debug.Stack()))
			res = execResult{panicked: true, panicValue: v}
		}
	}()
	return p.entry.run(ctx, row)
}

// record writes the job's next state and reports it.
func (p *kindPool) record(row store.ClaimedJob, a action, took time.Duration, logger *slog.Logger) {
	c, name := p.c, p.entry.cfg.Name
	ctx, cancel := context.WithTimeout(context.Background(), dbCallTimeout)
	defer cancel()

	ref := store.Ref{ID: row.ID, Lease: row.Lease}
	write := func() (bool, error) {
		if a.status == StatusPending {
			return c.st.Reschedule(ctx, ref, a.delay, a.errMsg, a.refund)
		}
		return c.st.Finish(ctx, ref, int16(a.status), a.errMsg)
	}
	// Transient failures are retried with doubling backoff, all within the
	// single dbCallTimeout budget of ctx.
	ok, err := write()
	retried := false
	for delay := statusWriteBackoff; err != nil && isTransient(err); delay *= 2 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		ok, err = write()
		retried = true
	}
	switch {
	case err != nil:
		logger.Warn("notasap: write job status", "status", a.status, "error", err)
		return
	case !ok && retried:
		// The retry cannot tell a committed earlier attempt from a lost
		// lease. The observer is not called, as for a lost lease.
		logger.Info("notasap: status write outcome unknown after retry", "status", a.status)
		return
	case !ok:
		logger.Warn("notasap: job lost lease", "status", a.status)
		return
	}

	switch a.status {
	case StatusPending:
		c.observer.JobRetried(name, a.reason, took)
		logger.Info("notasap: job retried", "reason", a.reason, "delay", a.delay, "error", a.errMsg)
	case StatusDone:
		c.observer.JobFinished(name, a.status, took)
		logger.Debug("notasap: job done", "took", took)
	case StatusCancelled:
		c.observer.JobFinished(name, a.status, took)
		logger.Info("notasap: job cancelled", "error", a.errMsg)
	default:
		c.observer.JobFinished(name, a.status, took)
		logger.Error("notasap: job failed", "reason", a.reason, "error", a.errMsg)
	}
}
