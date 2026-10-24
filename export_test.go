package notasap

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nottolstybai/not-asap/internal/store"
)

// NewWithDB is New with db used for queries instead of pool, for tests that
// inject failures into the database layer.
func NewWithDB(pool *pgxpool.Pool, db store.DB, opts ...Option) (*Client, error) {
	return newClient(store.New(pool), db, opts...)
}
