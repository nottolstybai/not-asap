package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const rescueSelectSQL = `SELECT id, kind, attempt, max_attempts FROM notasap_jobs
WHERE status = 2 AND deadline < NOW()
ORDER BY deadline
LIMIT $1
FOR UPDATE SKIP LOCKED`

const rescueFailSQL = `UPDATE notasap_jobs
SET status = 4, error = 'rescued: deadline exceeded', deadline = NULL, updated_at = NOW()
WHERE id = $1`

const rescueRetrySQL = `UPDATE notasap_jobs
SET status = 1, run_at = NOW() + make_interval(secs => $2),
    error = 'rescued: deadline exceeded', deadline = NULL, updated_at = NOW()
WHERE id = $1`

// StuckJob is a running job found past its deadline.
type StuckJob struct {
	ID          int64
	Kind        int16
	Attempt     int
	MaxAttempts int
}

// RescueDecision says what to do with a StuckJob: fail it, or return it to
// pending to run after Delay.
type RescueDecision struct {
	Fail  bool
	Delay time.Duration
}

// Rescue moves up to limit running jobs past their deadline to failed or
// pending, as decide says, in one transaction, and returns how many it moved.
// Rows locked by other transactions are skipped.
func (s *Store) Rescue(ctx context.Context, limit int, decide func(StuckJob) RescueDecision) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, rescueSelectSQL, limit)
		if err != nil {
			return err
		}
		jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (StuckJob, error) {
			var j StuckJob
			err := row.Scan(&j.ID, &j.Kind, &j.Attempt, &j.MaxAttempts)
			return j, err
		})
		if err != nil || len(jobs) == 0 {
			return err
		}

		b := &pgx.Batch{}
		for _, j := range jobs {
			if d := decide(j); d.Fail {
				b.Queue(rescueFailSQL, j.ID)
			} else {
				b.Queue(rescueRetrySQL, j.ID, d.Delay.Seconds())
			}
		}
		if err := tx.SendBatch(ctx, b).Close(); err != nil {
			return err
		}
		n = len(jobs)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("notasap: rescue: %w", err)
	}
	return n, nil
}
