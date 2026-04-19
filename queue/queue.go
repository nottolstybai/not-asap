package queue

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/semaphore"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
)

// Queue is the top-level library entry point. One Queue instance per application.
type Queue struct {
	db       *database.Client
	config   core.QueueConfig
	registry *core.Registry
	metrics  *core.Metrics

	// Worker pool
	workerID string
	sem      *semaphore.Weighted
	running  map[string]struct{}

	// Shutdown
	stopCh    chan struct{}
	wg        sync.WaitGroup
	jobCtx    context.Context
	jobCancel context.CancelFunc
	started   bool
	mu        sync.Mutex

	// Heartbeat
	runningMu sync.RWMutex
	hbDone    chan struct{}
	hbWg      sync.WaitGroup

	// Janitor
	janitorDone chan struct{}
	janitorWg   sync.WaitGroup

	// Shutdown idempotency
	shutdownOnce sync.Once
}

// NewQueue creates a Queue backed by the provided database.Client and config.
func NewQueue(db *database.Client, config core.QueueConfig) *Queue {
	return &Queue{
		db:       db,
		config:   config,
		registry: core.NewRegistry(),
		metrics:  core.NewMetrics(config.Registerer),
		running:  make(map[string]struct{}),
		stopCh:   make(chan struct{}),
		hbDone:   make(chan struct{}),
	}
}

// NewQueueFromPool creates a Queue from a raw *pgxpool.Pool for backward compatibility.
func NewQueueFromPool(pool *pgxpool.Pool, config core.QueueConfig) *Queue {
	return NewQueue(database.NewClient(pool), config)
}

// Pool returns the underlying *pgxpool.Pool for integration testing.
func (q *Queue) Pool() *pgxpool.Pool { return q.db.Pool() }

// resolveConfig returns the effective config for a job kind.
func (q *Queue) resolveConfig(kind string) core.EffectiveConfig {
	tc, _, ok := q.registry.Lookup(kind)
	if !ok {
		tc = core.TaskConfig{}
	}
	return core.ResolveTaskConfig(tc, q.config)
}

// Enqueue inserts one job without an external transaction.
func (q *Queue) Enqueue(ctx context.Context, args core.JobArgs) (*database.Job, error) {
	eff := q.resolveConfig(args.Kind())
	job, err := q.db.Enqueue(ctx, args.Kind(), args, eff.MaxAttempts)
	if err != nil {
		return nil, err
	}
	q.metrics.Enqueued.WithLabelValues(args.Kind()).Inc()
	return job, nil
}

// EnqueueBatch inserts multiple jobs in a single pgx.Batch round-trip.
func (q *Queue) EnqueueBatch(ctx context.Context, jobs ...core.JobArgs) error {
	if len(jobs) == 0 {
		return nil
	}
	batchJobs := make([]database.BatchJob, len(jobs))
	for i, j := range jobs {
		eff := q.resolveConfig(j.Kind())
		batchJobs[i] = database.BatchJob{
			Kind:        j.Kind(),
			Args:        j,
			MaxAttempts: eff.MaxAttempts,
		}
	}
	if err := q.db.EnqueueBatch(ctx, batchJobs); err != nil {
		return err
	}
	for _, j := range jobs {
		q.metrics.Enqueued.WithLabelValues(j.Kind()).Inc()
	}
	return nil
}

// EnqueueTx inserts one job inside the caller's pgx.Tx.
func (q *Queue) EnqueueTx(ctx context.Context, tx pgx.Tx, args core.JobArgs) (*database.Job, error) {
	eff := q.resolveConfig(args.Kind())
	job, err := q.db.EnqueueTx(ctx, tx, args.Kind(), args, eff.MaxAttempts)
	if err != nil {
		return nil, err
	}
	q.metrics.Enqueued.WithLabelValues(args.Kind()).Inc()
	return job, nil
}

// EnqueueBatchTx inserts multiple jobs inside the caller's pgx.Tx.
func (q *Queue) EnqueueBatchTx(ctx context.Context, tx pgx.Tx, jobs ...core.JobArgs) error {
	if len(jobs) == 0 {
		return nil
	}
	batchJobs := make([]database.BatchJob, len(jobs))
	for i, j := range jobs {
		eff := q.resolveConfig(j.Kind())
		batchJobs[i] = database.BatchJob{
			Kind:        j.Kind(),
			Args:        j,
			MaxAttempts: eff.MaxAttempts,
		}
	}
	if err := q.db.EnqueueBatchTx(ctx, tx, batchJobs); err != nil {
		return err
	}
	for _, j := range jobs {
		q.metrics.Enqueued.WithLabelValues(j.Kind()).Inc()
	}
	return nil
}

// Register registers a typed handler for job kind T.Kind().
// This is a wrapper around core.Register that takes *Queue.
func Register[T core.JobArgs](q *Queue, handler core.HandlerFunc[T], opts ...func(*core.TaskConfig)) {
	core.Register[T](q.registry, handler, opts...)
}

// LoggerFromContext extracts the enriched logger from a job's context.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	return core.LoggerFromContext(ctx)
}

func (q *Queue) logger() *slog.Logger {
	if q.config.Logger == nil {
		return slog.Default()
	}
	return q.config.Logger
}
