package notasap

import "errors"

var (
	// ErrCancel, returned by a handler, finishes the job as cancelled without retries.
	ErrCancel = errors.New("notasap: cancel")
	// ErrKeepAttempt, wrapped by a handler error, retries without consuming an attempt.
	ErrKeepAttempt = errors.New("notasap: keep attempt")
	// ErrUnknownKind means no handler is registered for the job kind.
	ErrUnknownKind = errors.New("notasap: unknown kind")
	// ErrDuplicate means a job with the same idempotency key already exists.
	ErrDuplicate = errors.New("notasap: duplicate job")
	// ErrJobRunning means the job is running and cannot be changed.
	ErrJobRunning = errors.New("notasap: job is running")
	// ErrJobFinished means the job already reached a final status.
	ErrJobFinished = errors.New("notasap: job is finished")
	// ErrNotFound means the job does not exist.
	ErrNotFound = errors.New("notasap: job not found")
	// ErrAlreadyStarted means the client was already started.
	ErrAlreadyStarted = errors.New("notasap: already started")
	// ErrStopped means the client was stopped.
	ErrStopped = errors.New("notasap: stopped")
)
