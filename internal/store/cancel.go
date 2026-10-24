package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const cancelSQL = `UPDATE notasap_jobs SET status = 5, updated_at = NOW()
WHERE id = $1 AND status = 1 RETURNING id`

const statusSQL = `SELECT status FROM notasap_jobs WHERE id = $1`

// CancelOutcome says what Cancel did.
type CancelOutcome int

// Cancel outcomes.
const (
	// CancelOK means a pending job was cancelled.
	CancelOK CancelOutcome = iota + 1
	// CancelRunning means the job is running and was left untouched.
	CancelRunning
	// CancelFinished means the job already had a final status.
	CancelFinished
	// CancelNotFound means no such job exists.
	CancelNotFound
)

// Cancel cancels the pending job id.
func (s *Store) Cancel(ctx context.Context, id int64) (CancelOutcome, error) {
	var got int64
	err := s.pool.QueryRow(ctx, cancelSQL, id).Scan(&got)
	if err == nil {
		return CancelOK, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("notasap: cancel: %w", err)
	}
	var status int16
	err = s.pool.QueryRow(ctx, statusSQL, id).Scan(&status)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return CancelNotFound, nil
	case err != nil:
		return 0, fmt.Errorf("notasap: cancel: %w", err)
	case status == statusRunning:
		return CancelRunning, nil
	default:
		return CancelFinished, nil
	}
}
