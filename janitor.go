package notasap

import (
	"context"
	"time"
)

// janitorStatuses is the order in which the janitor cleans statuses.
var janitorStatuses = [...]Status{StatusDone, StatusFailed, StatusCancelled}

// janitorLoop deletes old finished jobs: once at start, then every janitor
// Interval until the client stops.
func (c *Client) janitorLoop() {
	ticker := time.NewTicker(c.janitor.Interval)
	defer ticker.Stop()
	for {
		c.clean(c.bgCtx)
		select {
		case <-ticker.C:
		case <-c.stopCh:
			return
		}
	}
}

// clean deletes, status by status, jobs past their retention in batches
// until one comes back short or the client stops. A replica that loses the
// store's janitor advisory lock gets 0 rows, which counts as a short batch: it
// moves on to the next status and tries again on the next tick.
func (c *Client) clean(ctx context.Context) {
	for _, status := range janitorStatuses {
		retention := c.janitor.Retention[status]
		if retention <= 0 {
			continue
		}

		var total int64
		for ctx.Err() == nil {
			n, err := c.st.DeleteFinished(ctx, int16(status), retention, c.janitor.BatchSize)
			if err != nil {
				if ctx.Err() == nil {
					c.logger.Warn("notasap: delete finished jobs", "status", status, "error", err)
				}

				break
			}

			if n > 0 {
				c.observer.JanitorDeleted(status, n)
				total += n
			}

			if n < int64(c.janitor.BatchSize) {
				break
			}
		}

		if total > 0 {
			c.logger.Debug("notasap: janitor deleted finished jobs", "status", status, "count", total)
		}
	}
}
