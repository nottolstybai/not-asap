package queue

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"time"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
)

// finalCtx returns a bounded context for post-execution DB writes.
// We intentionally do NOT derive from q.jobCtx — that context is cancelled
// on shutdown, but the status UPDATE must still complete or the row is
// stuck in 'running' until the rescuer sees it.
func (q *Queue) finalCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// markJobDone sets status to 'done'.
func (q *Queue) markJobDone(job *database.ClaimedJob) {
	ctx, cancel := q.finalCtx()
	defer cancel()
	rowsAffected, err := q.db.CompleteJob(ctx, job.ID, q.workerID)
	if err != nil {
		q.logger().Warn("failed to mark job done", "job_id", job.ID, "error", err)
		return
	}
	if rowsAffected == 0 {
		q.logger().Warn("job already moved by rescuer", "job_id", job.ID)
		return
	}
	q.logger().Info("job completed", "job_id", job.ID, "job_type", job.JobType)
}

// markJobFailed sets status to 'failed' with the error message.
func (q *Queue) markJobFailed(job *database.ClaimedJob, errMsg string) {
	ctx, cancel := q.finalCtx()
	defer cancel()
	rowsAffected, err := q.db.FailJob(ctx, job.ID, q.workerID, errMsg)
	if err != nil {
		q.logger().Warn("failed to mark job as failed", "job_id", job.ID, "error", err)
		return
	}
	if rowsAffected == 0 {
		q.logger().Warn("job already moved by rescuer", "job_id", job.ID)
		return
	}
	q.logger().Warn("job failed",
		"job_id", job.ID,
		"error", errMsg,
		"attempt", job.Attempt,
		"max_attempts", job.MaxAttempts,
	)
}

// markJobCancelled sets status to 'cancelled' immediately.
func (q *Queue) markJobCancelled(job *database.ClaimedJob) {
	ctx, cancel := q.finalCtx()
	defer cancel()
	rowsAffected, err := q.db.CancelJob(ctx, job.ID, q.workerID)
	if err != nil {
		q.logger().Warn("failed to mark job as cancelled", "job_id", job.ID, "error", err)
		return
	}
	if rowsAffected == 0 {
		q.logger().Warn("job already moved by rescuer", "job_id", job.ID)
		return
	}
	q.logger().Info("job cancelled", "job_id", job.ID)
}

// markJobExhausted sets status to 'exhausted'.
func (q *Queue) markJobExhausted(job *database.ClaimedJob) {
	ctx, cancel := q.finalCtx()
	defer cancel()
	rowsAffected, err := q.db.ExhaustJob(ctx, job.ID, q.workerID)
	if err != nil {
		q.logger().Warn("failed to mark job as exhausted", "job_id", job.ID, "error", err)
		return
	}
	if rowsAffected == 0 {
		q.logger().Warn("job already moved by rescuer", "job_id", job.ID)
		return
	}
	q.logger().Error("job exhausted",
		"job_id", job.ID,
		"attempts", job.Attempt,
	)
}

// markJobRetrying sets status to 'retrying' with the calculated delay.
func (q *Queue) markJobRetrying(job *database.ClaimedJob, errMsg string, delay time.Duration) {
	runAfter := time.Now().Add(delay)
	ctx, cancel := q.finalCtx()
	defer cancel()
	rowsAffected, err := q.db.RetryJob(ctx, job.ID, q.workerID, errMsg, runAfter, job.Attempt+1)
	if err != nil {
		q.logger().Warn("failed to mark job as retrying", "job_id", job.ID, "error", err)
		return
	}
	if rowsAffected == 0 {
		q.logger().Warn("job already moved by rescuer", "job_id", job.ID)
		return
	}
	q.logger().Info("job retrying",
		"job_id", job.ID,
		"delay", delay,
		"next_attempt", job.Attempt+1,
	)
}

// handleRetry determines whether a failed job should be retried or marked exhausted.
// eff carries the per-task resolved retry configuration so TaskConfig overrides apply.
func (q *Queue) handleRetry(job *database.ClaimedJob, handlerErr error, eff core.EffectiveConfig) {
	nextAttempt := job.Attempt + 1

	if nextAttempt >= job.MaxAttempts {
		q.markJobExhausted(job)
		return
	}

	delay := calculateRetryDelay(
		eff.RetryStrategy,
		eff.RetryDelay,
		nextAttempt,
		eff.MaxRetryDelay,
	)

	errMsg := handlerErr.Error()
	if errors.Is(handlerErr, context.Canceled) {
		errMsg = "context cancelled (shutdown or manual cancel)"
	}
	if len(errMsg) > database.MaxErrorMsgLength {
		errMsg = errMsg[:database.MaxErrorMsgLength]
	}

	q.markJobRetrying(job, errMsg, delay)
}

// calculateRetryDelay computes the delay before the next retry attempt.
func calculateRetryDelay(strategy core.RetryStrategy, baseDelay time.Duration, attempt int, maxDelay time.Duration) time.Duration {
	switch strategy {
	case core.RetryStrategyStatic:
		return baseDelay

	case core.RetryStrategyExponential:
		expDelay := baseDelay
		for i := 1; i < attempt; i++ {
			expDelay *= 2
			if expDelay > maxDelay {
				expDelay = maxDelay
				break
			}
		}
		if expDelay > maxDelay {
			expDelay = maxDelay
		}
		n, err := rand.Int(rand.Reader, big.NewInt(int64(expDelay)+1))
		if err != nil {
			return expDelay
		}
		return time.Duration(n.Int64())

	default:
		return baseDelay
	}
}
