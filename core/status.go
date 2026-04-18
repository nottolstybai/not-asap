package core

import "errors"

// Status represents the lifecycle state of a job stored in notasap_jobs.status column.
// Type is string so it round-trips through the DB column without conversion and is human-readable in logs.
type Status string

const (
	StatusNew       Status = "new"
	StatusRunning   Status = "running"
	StatusDone      Status = "done"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
	StatusExhausted Status = "exhausted"
	StatusCancelled Status = "cancelled"
)

// ErrCancel is returned by a handler to cancel a job immediately without retrying.
// The job transitions to status='cancelled' regardless of remaining attempts.
// Implements LIFE-04 / WORK-05.
var ErrCancel = errors.New("notasap: job cancelled by handler")
