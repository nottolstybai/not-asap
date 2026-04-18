package database

import (
	"time"

	"github.com/nottolstybai/not-asap/core"
)

// Status is an alias for core.Status so Job.Status comparisons against
// core.StatusNew (exposed via notasap.StatusNew in the facade) succeed.
type Status = core.Status

// Re-export status constants for callers that only import database/.
const (
	StatusNew       = core.StatusNew
	StatusRunning   = core.StatusRunning
	StatusDone      = core.StatusDone
	StatusFailed    = core.StatusFailed
	StatusRetrying  = core.StatusRetrying
	StatusExhausted = core.StatusExhausted
	StatusCancelled = core.StatusCancelled
)

// Job is the record returned after a successful enqueue.
// Fields use `db` struct tags matching notasap_jobs column names for pgx.RowToStructByName.
type Job struct {
	ID          string    `db:"id"`
	JobType     string    `db:"job_type"`
	Status      Status    `db:"status"`
	MaxAttempts int       `db:"max_attempts"`
	CreatedAt   time.Time `db:"created_at"`
}

// ClaimedJob holds the data returned by a successful claim query.
type ClaimedJob struct {
	ID          string `db:"id"`
	JobType     string `db:"job_type"`
	Payload     []byte `db:"payload"`
	Attempt     int    `db:"attempt"`
	MaxAttempts int    `db:"max_attempts"`
}
