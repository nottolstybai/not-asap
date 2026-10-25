package notasap

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nottolstybai/not-asap/internal/store"
)

// NewWithDB is New with db used for queries instead of pool, for tests that
// inject failures into the database layer.
func NewWithDB(pool *pgxpool.Pool, db store.DB, opts ...Option) (*Client, error) {
	return newClient(store.New(pool), db, opts...)
}

// RescueNow runs one rescue pass with ctx.
func (c *Client) RescueNow(ctx context.Context) { c.rescue(ctx) }

// CleanNow runs one janitor pass with ctx.
func (c *Client) CleanNow(ctx context.Context) { c.clean(ctx) }
