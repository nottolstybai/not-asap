package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
)

// Start begins the worker pool. It launches WorkerCount goroutines that
// poll for and execute jobs concurrently. Start is idempotent.
func (q *Queue) Start() {
	q.mu.Lock()
	if q.started {
		q.mu.Unlock()
		return
	}
	q.started = true
	q.mu.Unlock()

	q.workerID = generateWorkerID()
	q.sem = newSemaphore(q.config.WorkerCount)
	q.jobCtx, q.jobCancel = context.WithCancel(context.Background())

	q.hbWg.Go(func() {
		q.heartbeater()
	})

	if !q.config.Janitor.Disabled {
		q.janitorDone = make(chan struct{})
		q.janitorWg.Go(func() {
			q.janitor()
		})
	}

	for range q.config.WorkerCount {
		go q.worker()
	}

	q.logger().Info("worker pool started",
		"worker_count", q.config.WorkerCount,
		"worker_id", q.workerID,
	)
}

// worker is the main loop for each worker goroutine.
func (q *Queue) worker() {
	for {
		select {
		case <-q.stopCh:
			return
		default:
		}

		if err := q.sem.Acquire(q.jobCtx, 1); err != nil {
			return
		}

		select {
		case <-q.stopCh:
			q.sem.Release(1)
			return
		default:
		}

		job, err := q.db.ClaimJob(q.jobCtx, q.workerID)
		if err != nil {
			q.sem.Release(1)
			if errors.Is(err, context.Canceled) {
				return
			}
			select {
			case <-time.After(q.config.PollInterval):
				continue
			case <-q.jobCtx.Done():
				return
			case <-q.stopCh:
				return
			}
		}
		if job == nil {
			q.sem.Release(1)
			select {
			case <-time.After(q.config.PollInterval):
				continue
			case <-q.jobCtx.Done():
				return
			case <-q.stopCh:
				return
			}
		}

		q.runningMu.Lock()
		q.running[job.ID] = struct{}{}
		q.runningMu.Unlock()

		q.wg.Add(1)
		go q.runJob(job)
	}
}

// runJob executes a claimed job. All outcomes (success, handler error,
// ErrCancel, panic) must be reported from within the deferred function —
// a panic inside handle() skips every statement that follows it.
func (q *Queue) runJob(job *database.ClaimedJob) {
	eff := q.resolveConfig(job.JobType)

	var handlerErr error
	var panicValue any
	var panicStack []byte
	handlerRan := false

	defer func() {
		if r := recover(); r != nil {
			panicValue = r
			panicStack = debug.Stack()
		}

		q.runningMu.Lock()
		delete(q.running, job.ID)
		q.runningMu.Unlock()
		q.sem.Release(1)
		q.wg.Done()

		switch {
		case panicValue != nil:
			q.logger().Error("job panicked",
				"job_id", job.ID,
				"panic_value", fmt.Sprintf("%v", panicValue),
				"stack_trace", string(panicStack),
			)
			q.markJobFailed(job, fmt.Sprintf("panic: %v", panicValue))
		case !handlerRan:
			// lookup miss — already reported
		case handlerErr == nil:
			q.markJobDone(job)
		case errors.Is(handlerErr, core.ErrCancel):
			q.markJobCancelled(job)
		default:
			q.handleRetry(job, handlerErr, eff)
		}
	}()

	_, handle, ok := q.registry.Lookup(job.JobType)
	if !ok {
		q.markJobFailed(job, "notasap: no handler registered for job_type "+job.JobType)
		return
	}

	logger := q.config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	parentCtx := q.jobCtx
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	jobCtx := context.WithValue(parentCtx, core.SlogValueKey{}, logger.With(
		"job_id", job.ID,
		"job_type", job.JobType,
		"attempt", job.Attempt,
	))

	handlerRan = true
	handlerErr = handle(jobCtx, job.Payload)
}
