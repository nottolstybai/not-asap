package orders

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repo struct {
	pool *pgxpool.Pool
}

func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// Insert inserts a new order inside the caller's transaction and returns the new row.
// Note: Insert does not commit — the caller composes with queue.EnqueueTx and commits both.
func (r *Repo) Insert(ctx context.Context, tx pgx.Tx, customer, address string, amount float64) (Order, error) {
	const q = `
INSERT INTO orders (customer, address, amount)
VALUES ($1, $2, $3)
RETURNING id, customer, address, amount, created_at`

	rows, err := tx.Query(ctx, q, customer, address, amount)
	if err != nil {
		return Order{}, err
	}
	return pgx.CollectOneRow(rows, pgx.RowToStructByName[Order])
}

// List returns the N most recent orders (for UI rendering).
func (r *Repo) List(ctx context.Context, limit int) ([]Order, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, customer, address, amount, created_at FROM orders ORDER BY created_at DESC LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByName[Order])
}
