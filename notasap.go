package notasap

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
	"github.com/nottolstybai/not-asap/queue"
)

// Type aliases for backward compatibility.
type Queue = queue.Queue
type Job = database.Job
type JobArgs = core.JobArgs
type HandlerFunc[T JobArgs] = core.HandlerFunc[T]
type Status = core.Status
type QueueConfig = core.QueueConfig
type TaskConfig = core.TaskConfig
type JanitorConfig = core.JanitorConfig
type RetryStrategy = core.RetryStrategy

// Function re-exports.
func Register[T JobArgs](q *Queue, handler HandlerFunc[T], opts ...func(*TaskConfig)) {
	queue.Register[T](q, handler, opts...)
}

var LoggerFromContext = core.LoggerFromContext
var NewQueueConfig = core.NewQueueConfig

// NewQueue creates a Queue from a raw *pgxpool.Pool for backward compatibility.
func NewQueue(pool *pgxpool.Pool, config QueueConfig) *Queue {
	return queue.NewQueueFromPool(pool, config)
}

// Variable re-exports.
var ErrCancel = core.ErrCancel
var MigrationFiles = database.MigrationFiles

// Constant re-exports.
const (
	StatusNew       = core.StatusNew
	StatusRunning   = core.StatusRunning
	StatusDone      = core.StatusDone
	StatusFailed    = core.StatusFailed
	StatusRetrying  = core.StatusRetrying
	StatusExhausted = core.StatusExhausted
	StatusCancelled = core.StatusCancelled
)

const (
	RetryStrategyStatic      = core.RetryStrategyStatic
	RetryStrategyExponential = core.RetryStrategyExponential
)

// Re-export types for pgx interop.
type Tx = pgx.Tx
