package queue

import (
	"time"

	"github.com/nottolstybai/not-asap/database"
)

// janitor runs periodically to clean up terminal jobs.
func (q *Queue) janitor() {
	ticker := time.NewTicker(q.config.Janitor.RunInterval)
	defer ticker.Stop()

	q.logger().Info("janitor started",
		"ttl", q.config.Janitor.TTL,
		"statuses", q.config.Janitor.Statuses,
		"interval", q.config.Janitor.RunInterval,
	)

	for {
		select {
		case <-q.janitorDone:
			q.logger().Info("janitor stopped")
			return
		case <-ticker.C:
			start := time.Now()
			deleted, err := q.janitorCycle()
			duration := time.Since(start)
			if err != nil {
				q.logger().Error("janitor cycle failed", "error", err)
			} else {
				q.logger().Debug("janitor cycle complete",
					"deleted_count", deleted,
					"duration_ms", duration.Milliseconds(),
				)
			}
		}
	}
}

// janitorCycle performs one cleanup pass, deleting terminal jobs in batches.
func (q *Queue) janitorCycle() (int64, error) {
	var totalDeleted int64

	for _, status := range q.config.Janitor.Statuses {
		for {
			deleted, err := q.db.JanitorDeleteBatch(q.jobCtx, database.Status(status), q.config.Janitor.TTL)
			if err != nil {
				return totalDeleted, err
			}
			totalDeleted += deleted
			if deleted == 0 {
				break
			}
		}
	}
	return totalDeleted, nil
}
