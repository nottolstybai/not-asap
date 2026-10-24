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
	JobScheduled(kind string)
	JobStarted(kind string)
	JobEnded(kind string)
	JobFinished(kind string, status Status, took time.Duration)
	JobRetried(kind string, reason RetryReason, took time.Duration)
	JobStuck(kind string)
	JobRescued(kind string)
	Polled(kind string, took time.Duration)
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
