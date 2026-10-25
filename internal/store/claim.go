package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const claimSQL = `UPDATE notasap_jobs j
SET status = 2 /* running */, attempt = j.attempt + 1, lease = j.lease + 1,
    deadline = NOW() + make_interval(secs => $3), updated_at = NOW()
FROM (
    SELECT id FROM notasap_jobs
    WHERE status = 1 /* pending */ AND kind = $1 AND run_at <= NOW()
    ORDER BY run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
) c
WHERE j.id = c.id
RETURNING j.id, j.kind, j.payload, j.attempt, j.max_attempts, j.lease, j.created_at`

const finishSQL = `UPDATE notasap_jobs
SET status = $3, error = $4, deadline = NULL, updated_at = NOW()
WHERE id = $1 AND lease = $2 AND status = 2 /* running */`

const rescheduleSQL = `UPDATE notasap_jobs
SET status = 1 /* pending */, run_at = NOW() + make_interval(secs => $3), error = $4, deadline = NULL,
    attempt = CASE WHEN $5 THEN attempt - 1 ELSE attempt END, updated_at = NOW()
WHERE id = $1 AND lease = $2 AND status = 2 /* running */`

// ClaimedJob is a job row taken for execution.
type ClaimedJob struct {
	ID          int64
	Kind        int16
	Payload     []byte
	Attempt     int
	MaxAttempts int `db:"max_attempts"`
	Lease       int64
	CreatedAt   time.Time `db:"created_at"`
}

// Ref identifies one claim of a job: Lease fences out stale owners.
type Ref struct {
	ID, Lease int64
}

// Claim moves up to limit due pending jobs of kind to running, giving each a
// new lease valid for the lease duration, and returns them.
func (s *Store) Claim(ctx context.Context, kind int16, limit int, lease time.Duration) ([]ClaimedJob, error) {
	rows, err := s.pool.Query(ctx, claimSQL, kind, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("pool.Query: %w", err)
	}

	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[ClaimedJob])
	if err != nil {
		return nil, fmt.Errorf("pgx.CollectRows: %w", err)
	}

	return jobs, nil
}

// Finish moves a running job to a final status. It reports false if the lease
// no longer matches (the job was rescued or already finished). An empty errMsg
// stores NULL.
func (s *Store) Finish(ctx context.Context, ref Ref, status int16, errMsg string) (bool, error) {
	tag, err := s.pool.Exec(ctx, finishSQL, ref.ID, ref.Lease, status, nullable(errMsg))
	if err != nil {
		return false, fmt.Errorf("pool.Exec: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}

// Reschedule returns a running job to pending to run after delay. With refund
// the attempt consumed by the claim is given back. It reports false if the
// lease no longer matches. An empty errMsg stores NULL.
func (s *Store) Reschedule(ctx context.Context, ref Ref, delay time.Duration, errMsg string, refund bool) (bool, error) {
	tag, err := s.pool.Exec(ctx, rescheduleSQL, ref.ID, ref.Lease, delay.Seconds(), nullable(errMsg), refund)
	if err != nil {
		return false, fmt.Errorf("pool.Exec: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
