package notasap

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nottolstybai/not-asap/internal/store"
)

// jobStore is the persistence contract of the Client; methods are added as
// the features that need them are introduced.
type jobStore interface {
	Insert(ctx context.Context, db store.DB, params []store.InsertParams) ([]store.InsertResult, error)
	Claim(ctx context.Context, kind int16, limit int, lease time.Duration) ([]store.ClaimedJob, error)
	Finish(ctx context.Context, ref store.Ref, status int16, errMsg string) (bool, error)
	Reschedule(ctx context.Context, ref store.Ref, delay time.Duration, errMsg string, refund bool) (bool, error)
	Cancel(ctx context.Context, id int64) (store.CancelOutcome, error)
	Rescue(ctx context.Context, limit int, decide func(store.StuckJob) store.RescueDecision) (int, error)
	CheckSchema(ctx context.Context) error
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

	// Set by Start. stopCh and bgCtx stop pollers and maintenance loops;
	// runCtx is the parent of handler contexts, cancelled with errShutdown
	// when Stop runs out of time.
	stopCh    chan struct{}
	bgCtx     context.Context
	cancelBg  context.CancelFunc
	runCtx    context.Context
	cancelRun context.CancelCauseFunc
	bgWG      sync.WaitGroup // pollers and maintenance loops
	jobsWG    sync.WaitGroup // running handlers
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

// Start checks that the jobs table is reachable and starts the worker pools
// and maintenance loops. It does not block; ctx bounds only the startup check.
// A Client can be started once: a second call returns ErrAlreadyStarted, a
// call after Stop returns ErrStopped. If the check fails, Start may be retried.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case stateStarted:
		return ErrAlreadyStarted
	case stateStopped:
		return ErrStopped
	}
	if err := c.st.CheckSchema(ctx); err != nil {
		return err
	}

	c.stopCh = make(chan struct{})
	c.bgCtx, c.cancelBg = context.WithCancel(context.Background())
	c.runCtx, c.cancelRun = context.WithCancelCause(context.Background())
	c.state = stateStarted

	names := make([]string, 0, len(c.kinds))
	for _, e := range c.kinds {
		p := &kindPool{
			c:     c,
			e:     e,
			slots: make(chan struct{}, e.cfg.Workers),
			wake:  make(chan struct{}, 1),
		}
		c.bgWG.Go(p.loop)
		names = append(names, e.cfg.Name)
	}
	c.startMaintenance()

	slices.Sort(names)
	c.logger.Info("notasap started", "kinds", names)
	return nil
}

// startMaintenance starts the background maintenance loops in c.bgWG.
func (c *Client) startMaintenance() {
	c.bgWG.Go(c.rescueLoop)
}

// Stop stops polling and maintenance and waits for running handlers until ctx
// is done. Then it cancels handler contexts, waits up to the stop grace for
// them to record their status and returns ctx.Err(). Handlers that ignore
// their context may still be running after Stop returns; their jobs are
// recovered by the rescuer. Stop on a Client that is not running returns nil.
func (c *Client) Stop(ctx context.Context) error {
	c.mu.Lock()
	started := c.state == stateStarted
	c.state = stateStopped
	c.mu.Unlock()
	if !started {
		return nil
	}

	close(c.stopCh)
	c.cancelBg()
	c.bgWG.Wait() // no pollers left, so jobsWG cannot grow any more

	drained := make(chan struct{})
	go func() {
		c.jobsWG.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-ctx.Done():
	}
	select {
	case <-drained:
		c.cancelRun(errShutdown)
		c.logger.Info("notasap stopped")
		return nil
	default:
	}

	c.cancelRun(errShutdown)
	grace := time.NewTimer(c.stopGrace)
	defer grace.Stop()
	select {
	case <-drained:
	case <-grace.C:
		c.logger.Warn("notasap: handlers still running after stop grace")
	}
	c.logger.Info("notasap stopped")
	return ctx.Err()
}
