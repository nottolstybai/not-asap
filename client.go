package notasap

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nottolstybai/not-asap/internal/store"
)

// jobStore is the persistence contract of the Client; methods are added as
// the features that need them are introduced.
type jobStore interface {
	Insert(ctx context.Context, db store.DB, params []store.InsertParams) ([]store.InsertResult, error)
}

var _ jobStore = (*store.Store)(nil)

type clientState int

const (
	stateNew clientState = iota
	stateStarted
	stateStopped
)

// Client schedules and runs jobs.
type Client struct {
	options
	st    jobStore
	db    store.DB
	mu    sync.Mutex
	state clientState
	kinds map[Kind]*kindEntry
	names map[string]Kind
}

// New creates a Client over pool. It validates all options and returns the
// first error found.
func New(pool *pgxpool.Pool, opts ...Option) (*Client, error) {
	if pool == nil {
		return nil, errors.New("notasap: New: pool is nil")
	}
	return newClient(store.New(pool), pool, opts...)
}

func newClient(st jobStore, db store.DB, opts ...Option) (*Client, error) {
	o := newOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	return &Client{
		options: o,
		st:      st,
		db:      db,
		kinds:   make(map[Kind]*kindEntry),
		names:   make(map[string]Kind),
	}, nil
}
