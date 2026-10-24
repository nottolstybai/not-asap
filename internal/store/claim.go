package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const claimSQL = `UPDATE notasap_jobs j
SET status = 2, attempt = j.attempt + 1, lease = j.lease + 1,
    deadline = NOW() + make_interval(secs => $3), updated_at = NOW()
FROM (
    SELECT id FROM notasap_jobs
    WHERE status = 1 AND kind = $1 AND run_at <= NOW()
    ORDER BY run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
) c
WHERE j.id = c.id
RETURNING j.id, j.kind, j.payload, j.attempt, j.max_attempts, j.lease, j.created_at`

const finishSQL = `UPDATE notasap_jobs
SET status = $3, error = $4, deadline = NULL, updated_at = NOW()
WHERE id = $1 AND lease = $2 AND status = 2`

const rescheduleSQL = `UPDATE notasap_jobs
SET status = 1, run_at = NOW() + make_interval(secs => $3), error = $4, deadline = NULL,
    attempt = CASE WHEN $5 THEN attempt - 1 ELSE attempt END, updated_at = NOW()
WHERE id = $1 AND lease = $2 AND status = 2`

// Ref identifies one claim of a job: Lease fences out stale owners.
type Ref struct {
	ID, Lease int64
}

// Claim moves up to limit due pending jobs of kind to running, giving each a
// new lease valid for the lease duration, and returns them.
func (s *Store) Claim(ctx context.Context, kind int16, limit int, lease time.Duration) ([]ClaimedJob, error) {
	rows, err := s.pool.Query(ctx, claimSQL, kind, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("notasap: claim: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (ClaimedJob, error) {
		var j ClaimedJob
		err := row.Scan(&j.ID, &j.Kind, &j.Payload, &j.Attempt, &j.MaxAttempts, &j.Lease, &j.CreatedAt)
		return j, err
	})
	if err != nil {
		return nil, fmt.Errorf("notasap: claim: %w", err)
	}
	return jobs, nil
}

// Finish moves a running job to a final status. It reports false if the lease
// no longer matches (the job was rescued or already finished). An empty errMsg
// stores NULL.
func (s *Store) Finish(ctx context.Context, ref Ref, status int16, errMsg string) (bool, error) {
	tag, err := s.pool.Exec(ctx, finishSQL, ref.ID, ref.Lease, status, nullable(errMsg))
	if err != nil {
		return false, fmt.Errorf("notasap: finish: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Reschedule returns a running job to pending to run after delay. With refund
// the attempt consumed by the claim is given back. It reports false if the
// lease no longer matches. An empty errMsg stores NULL.
func (s *Store) Reschedule(ctx context.Context, ref Ref, delay time.Duration, errMsg string, refund bool) (bool, error) {
	tag, err := s.pool.Exec(ctx, rescheduleSQL, ref.ID, ref.Lease, delay.Seconds(), nullable(errMsg), refund)
	if err != nil {
		return false, fmt.Errorf("notasap: reschedule: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// CheckSchema verifies that the notasap_jobs table is reachable.
func (s *Store) CheckSchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "SELECT 1 FROM notasap_jobs LIMIT 0")
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
		return errors.New("notasap: table notasap_jobs not found; apply migrations")
	}
	if err != nil {
		return fmt.Errorf("notasap: check schema: %w", err)
	}
	return nil
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
