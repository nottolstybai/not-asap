package notasap

import (
	"strconv"

	"github.com/nottolstybai/not-asap/internal/store"
)

// Kind identifies a job type. Each Kind is handled by exactly one registered handler.
type Kind int16

// Payload is a job argument. Its Kind selects the handler that runs the job.
type Payload interface {
	Kind() Kind
}

// Status is the lifecycle state of a job.
type Status int16

// Job statuses as stored in the database.
const (
	StatusPending   Status = Status(store.StatusPending)
	StatusRunning   Status = Status(store.StatusRunning)
	StatusDone      Status = Status(store.StatusDone)
	StatusFailed    Status = Status(store.StatusFailed)
	StatusCancelled Status = Status(store.StatusCancelled)
)

// String returns the lowercase status name, or "status(N)" for unknown values.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusDone:
		return "done"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	}

	return "status(" + strconv.Itoa(int(s)) + ")"
}
