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

// RescueNow runs one rescue pass with ctx. It may overlap the pass the
// background loop runs at Start: that is DB-safe (SKIP LOCKED), but observer
// counts and row state can be affected. Tests that need no handlers call it
// on a client that was never started, so no other pass exists. Otherwise
// start the client with a rescuer Interval of 1h and prepare rows (a future
// deadline, then SetJob it into the past) so the initial pass does not touch
// them.
func (c *Client) RescueNow(ctx context.Context) { c.rescue(ctx) }

// CleanNow runs one janitor pass with ctx. The same overlap caveat as
// RescueNow applies; calling it on a client that was never started avoids it.
func (c *Client) CleanNow(ctx context.Context) { c.clean(ctx) }
