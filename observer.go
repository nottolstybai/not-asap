package notasap

import "time"

// RetryReason says why a job attempt was sent back for another try.
type RetryReason string

// Retry reasons reported to Observer.JobRetried.
const (
	RetryError       RetryReason = "error"
	RetryPanic       RetryReason = "panic"
	RetryTimeout     RetryReason = "timeout"
	RetryKeepAttempt RetryReason = "keep_attempt"
	RetryShutdown    RetryReason = "shutdown"
)

// Observer receives job lifecycle events, e.g. for metrics. Implementations
// must be safe for concurrent use and must not block.
type Observer interface {
	// JobScheduled is called for every inserted or replaced job, not for
	// duplicates. With ScheduleTx and ScheduleBatchTx it is called before
	// the caller's tx commits, so it also counts jobs whose tx rolls back.
	JobScheduled(kind string)
	// JobStarted is called before a handler runs. Every JobStarted is
	// followed by one JobEnded, so the pair tracks busy worker slots.
	JobStarted(kind string)
	// JobEnded is called once a handler returned and its result is queued
	// for writing, whatever the outcome, so it may come before JobFinished
	// or JobRetried.
	JobEnded(kind string)
	// JobFinished is called once a job's final status (done, failed or
	// cancelled) is written, took being the handler run time. It is not
	// called when the lease was lost or, after a retried status write, the
	// outcome is unknown.
	JobFinished(kind string, status Status, took time.Duration)
	// JobRetried is called once a failed attempt is written back to pending,
	// with the same exceptions as JobFinished. Jobs returned by the rescuer
	// are reported by JobRescued instead.
	JobRetried(kind string, reason RetryReason, took time.Duration)
	// JobStuck is called when a handler is still running at Timeout plus the
	// rescuer Grace, that is, it ignores its context.
	JobStuck(kind string)
	// JobRescued is called after the rescuer committed a job past its
	// deadline back to pending or to failed. kind is "kind_N" for kinds with
	// no registered handler.
	JobRescued(kind string)
	// Polled is called after every claim query, including failed ones.
	Polled(kind string, took time.Duration)
	// JanitorDeleted is called after a janitor batch deleted n > 0 jobs.
	JanitorDeleted(status Status, n int64)
}

// NopObserver implements Observer with no-ops. Embed it to override selected methods.
type NopObserver struct{}

var _ Observer = NopObserver{}

func (NopObserver) JobScheduled(string)                           {}
func (NopObserver) JobStarted(string)                             {}
func (NopObserver) JobEnded(string)                               {}
func (NopObserver) JobFinished(string, Status, time.Duration)     {}
func (NopObserver) JobRetried(string, RetryReason, time.Duration) {}
func (NopObserver) JobStuck(string)                               {}
func (NopObserver) JobRescued(string)                             {}
func (NopObserver) Polled(string, time.Duration)                  {}
func (NopObserver) JanitorDeleted(Status, int64)                  {}
