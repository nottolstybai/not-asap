// Package store holds the Postgres access layer of notasap.
package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is the subset of pgx used by the store. It is satisfied by
// *pgxpool.Pool and pgx.Tx.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// statusRunning is the running value of the status column.
const statusRunning = 2

// Store executes notasap queries against a pool.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// ClaimedJob is a job row taken for execution.
type ClaimedJob struct {
	ID          int64
	Kind        int16
	Payload     []byte
	Attempt     int
	MaxAttempts int
	Lease       int64
	CreatedAt   time.Time
}
