package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
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

const completeSQL = `UPDATE notasap_jobs j
SET status = u.status,
    run_at = CASE WHEN u.status = 1 /* pending */ THEN NOW() + make_interval(secs => u.delay) ELSE j.run_at END,
    error = u.error, deadline = NULL,
    attempt = CASE WHEN u.refund THEN j.attempt - 1 ELSE j.attempt END, updated_at = NOW()
FROM unnest($1::bigint[], $2::bigint[], $3::smallint[], $4::text[], $5::float8[], $6::bool[])
    AS u(id, lease, status, error, delay, refund)
WHERE j.id = u.id AND j.lease = u.lease AND j.status = 2 /* running */
RETURNING j.id, j.lease`

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
// the rows written. A missing ref means its lease no longer matches (the job
// was rescued or already finished). Rows are updated in id order so that
// concurrent batches do not deadlock.
func (s *Store) Complete(ctx context.Context, cs []Completion) ([]Ref, error) {
	cs = slices.SortedFunc(slices.Values(cs), func(a, b Completion) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.Lease, b.Lease))
	})
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
