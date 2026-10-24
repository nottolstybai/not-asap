package notasap

import (
	"fmt"
	"time"

	"github.com/nottolstybai/not-asap/internal/store"
)

// rescueBatch is the maximum number of jobs rescued per transaction.
const rescueBatch = 100

// rescued is a job moved by one rescue pass, reported after commit.
type rescued struct {
	job  store.StuckJob
	name string
	d    store.RescueDecision
}

// rescueLoop returns running jobs past their deadline to pending or failed:
// once at start, then every rescuer Interval until the client stops.
func (c *Client) rescueLoop() {
	ticker := time.NewTicker(c.rescuer.Interval)
	defer ticker.Stop()
	for {
		c.rescue()
		select {
		case <-ticker.C:
		case <-c.stopCh:
			return
		}
	}
}

// rescue runs rescue batches until one comes back short.
func (c *Client) rescue() {
	for {
		var batch []rescued
		n, err := c.st.Rescue(c.bgCtx, rescueBatch, func(j store.StuckJob) store.RescueDecision {
			r := c.rescueDecision(j)
			batch = append(batch, r)
			return r.d
		})
		if err != nil {
			if c.bgCtx.Err() == nil {
				c.logger.Warn("notasap: rescue jobs", "error", err)
			}
			return
		}
		for _, r := range batch {
			c.observer.JobRescued(r.name)
			status := StatusPending
			if r.d.Fail {
				status = StatusFailed
			}
			c.logger.Warn("notasap: job rescued after deadline",
				"job_id", r.job.ID, "kind", r.name, "attempt", r.job.Attempt,
				"status", status, "delay", r.d.Delay)
		}
		if n < rescueBatch {
			return
		}
	}
}

// rescueDecision fails a stuck job with no attempts left and otherwise
// retries it after its kind's backoff; unregistered kinds use the defaults.
func (c *Client) rescueDecision(j store.StuckJob) rescued {
	name, backoff := fmt.Sprintf("kind_%d", j.Kind), c.defaults.Backoff
	if e, ok := c.entry(Kind(j.Kind)); ok {
		name, backoff = e.cfg.Name, e.cfg.Backoff
	}
	r := rescued{job: j, name: name}
	if j.Attempt >= j.MaxAttempts {
		r.d.Fail = true
	} else {
		r.d.Delay = delayOf(backoff, j.Attempt)
	}
	return r
}
