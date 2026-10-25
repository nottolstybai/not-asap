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

// completeSQL locks the rows to write in id order, so that concurrent batches
// do not deadlock, and skips rows locked by another transaction, so that one
// such row does not hold up the whole batch.
const completeSQL = `WITH u AS (
    SELECT id, lease, status, error, delay, refund
    FROM unnest($1::bigint[], $2::bigint[], $3::smallint[], $4::text[], $5::float8[], $6::bool[])
        AS u(id, lease, status, error, delay, refund)
), l AS MATERIALIZED (
    SELECT j.id FROM notasap_jobs j JOIN u ON j.id = u.id AND j.lease = u.lease
    WHERE j.status = 2 /* running */
    ORDER BY j.id
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE notasap_jobs j
SET status = u.status,
    run_at = CASE WHEN u.status = 1 /* pending */ THEN NOW() + make_interval(secs => u.delay) ELSE j.run_at END,
    error = u.error, deadline = NULL,
    attempt = CASE WHEN u.refund THEN j.attempt - 1 ELSE j.attempt END, updated_at = NOW()
FROM u JOIN l USING (id)
WHERE j.id = u.id AND j.lease = u.lease
RETURNING j.id, j.lease`

// completeOneSQL is completeSQL for one row, waiting for its lock.
const completeOneSQL = `UPDATE notasap_jobs
SET status = $3,
    run_at = CASE WHEN $3::smallint = 1 /* pending */ THEN NOW() + make_interval(secs => $5) ELSE run_at END,
    error = $4, deadline = NULL,
    attempt = CASE WHEN $6 THEN attempt - 1 ELSE attempt END, updated_at = NOW()
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

// Completion is the next state of one claimed job.
type Completion struct {
	Ref
	// Status is final, or StatusPending to run again after Delay.
	Status int16
	Delay  time.Duration
	// Error is stored as NULL when empty.
	Error string
	// Refund gives back the attempt consumed by the claim; only for
	// StatusPending.
	Refund bool
}

// Complete writes all completions in one statement and returns the refs of
// the rows written. Rows are locked in id order, so concurrent batches do not
// deadlock. A ref is missing when its lease no longer matches (the job was
// rescued or already finished) or its row is locked by another transaction;
// CompleteOne tells the two apart.
func (s *Store) Complete(ctx context.Context, cs []Completion) ([]Ref, error) {
	ids := make([]int64, len(cs))
	leases := make([]int64, len(cs))
	statuses := make([]int16, len(cs))
	errs := make([]*string, len(cs))
	delays := make([]float64, len(cs))
	refunds := make([]bool, len(cs))
	for i, c := range cs {
		ids[i], leases[i], statuses[i] = c.ID, c.Lease, c.Status
		errs[i], delays[i], refunds[i] = nullable(c.Error), c.Delay.Seconds(), c.Refund
	}

	rows, err := s.pool.Query(ctx, completeSQL, ids, leases, statuses, errs, delays, refunds)
	if err != nil {
		return nil, fmt.Errorf("pool.Query: %w", err)
	}

	written, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Ref])
	if err != nil {
		return nil, fmt.Errorf("pgx.CollectRows: %w", err)
	}

	return written, nil
}

// CompleteOne writes c like Complete, but waits for a row lock held by
// another transaction. It reports false if the lease no longer matches.
func (s *Store) CompleteOne(ctx context.Context, c Completion) (bool, error) {
	tag, err := s.pool.Exec(ctx, completeOneSQL, c.ID, c.Lease, c.Status, nullable(c.Error), c.Delay.Seconds(), c.Refund)
	if err != nil {
		return false, fmt.Errorf("pool.Exec: %w", err)
	}

	return tag.RowsAffected() == 1, nil
}
