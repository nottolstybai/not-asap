package queue

import (
	"context"
	"time"
)

// heartbeater runs in its own goroutine and periodically batch-updates
// last_heartbeat_at for all currently running jobs.
func (q *Queue) heartbeater() {
	ticker := time.NewTicker(q.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-q.hbDone:
			return
		case <-ticker.C:
			q.heartbeatTick()
		}
	}
}

// heartbeatTick snapshots the running job IDs and issues a single batched UPDATE.
func (q *Queue) heartbeatTick() {
	q.runningMu.RLock()
	if len(q.running) == 0 {
		q.runningMu.RUnlock()
		return
	}
	ids := make([]string, 0, len(q.running))
	for id := range q.running {
		ids = append(ids, id)
	}
	q.runningMu.RUnlock()

	if err := q.db.HeartbeatBatch(context.Background(), ids); err != nil {
		q.logger().Warn("heartbeat update failed", "error", err, "job_count", len(ids))
	}
}
