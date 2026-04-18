package queue

import (
	"context"
	"time"
)

// stopHeartbeater signals the heartbeater goroutine to stop and waits for it to exit.
func (q *Queue) stopHeartbeater() {
	close(q.hbDone)
	q.hbWg.Wait()
}

// Stop performs a graceful shutdown of the worker pool.
//
// Phase 1 (stop polling): Closes stopCh, signaling all worker goroutines to
// stop claiming new jobs. Stops the heartbeater.
//
// Phase 2 (drain in-flight): Waits for currently executing jobs to complete,
// up to ctx.Deadline() or ShutdownTimeout. If the deadline is exceeded,
// all remaining job contexts are cancelled.
//
// Interrupted jobs transition to status='retrying' — they are not lost.
//
// Stop is idempotent — calling it multiple times returns immediately.
func (q *Queue) Stop(ctx context.Context) error {
	var stopErr error
	q.shutdownOnce.Do(func() {
		stopErr = q.stop(ctx)
	})
	return stopErr
}

func (q *Queue) stop(ctx context.Context) error {
	q.mu.Lock()
	if !q.started {
		q.mu.Unlock()
		return nil
	}
	q.mu.Unlock()

	q.logger().Info("worker pool stopping", "worker_id", q.workerID)

	close(q.stopCh)
	q.stopHeartbeater()

	if !q.config.Janitor.Disabled && q.janitorDone != nil {
		close(q.janitorDone)
		q.janitorWg.Wait()
	}

	drainCtx, drainCancel := context.WithTimeout(ctx, q.config.ShutdownTimeout)
	defer drainCancel()

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		q.jobCancel()
		q.logger().Info("worker pool stopped", "worker_id", q.workerID)
		return nil

	case <-drainCtx.Done():
		q.jobCancel()
		select {
		case <-done:
			q.logger().Info("worker pool stopped", "worker_id", q.workerID)
			return drainCtx.Err()
		case <-time.After(1 * time.Second):
			q.logger().Info("worker pool stopped (timeout)", "worker_id", q.workerID)
			return drainCtx.Err()
		}
	}
}
