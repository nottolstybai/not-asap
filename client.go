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

// jobStore is the persistence layer of the Client: *store.Store in
// production, fakeStore in unit tests.
type jobStore interface {
	Insert(ctx context.Context, db store.DB, params []store.InsertParams) ([]store.InsertResult, error)
	Claim(ctx context.Context, kind int16, limit int, lease time.Duration) ([]store.ClaimedJob, error)
	Finish(ctx context.Context, ref store.Ref, status int16, errMsg string) (bool, error)
	Reschedule(ctx context.Context, ref store.Ref, delay time.Duration, errMsg string, refund bool) (bool, error)
	Cancel(ctx context.Context, id int64) (store.CancelOutcome, error)
	Rescue(ctx context.Context, limit int, decide func(store.StuckJob) store.RescueDecision) (int, error)
	DeleteFinished(ctx context.Context, status int16, olderThan time.Duration, limit int) (int64, error)
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
	// stopped is closed when the first Stop returns.
	stopped chan struct{}
	kinds   map[Kind]*kindEntry
	names   map[string]Kind
	// maxConns is the pool's MaxConns; 0 means unknown.
	maxConns int32

	// Set by Start. stopCh and bgCtx stop pollers and maintenance loops;
	// runCtx is the parent of handler contexts, cancelled with errShutdown
	// when Stop runs out of time.
	stopCh    chan struct{}
	bgCtx     context.Context
	cancelBg  context.CancelFunc
	runCtx    context.Context
	cancelRun context.CancelCauseFunc
	// pools holds the poller of every kind; Schedule wakes them through it.
	pools  map[Kind]*kindPool
	bgWG   sync.WaitGroup // pollers and maintenance loops
	jobsWG sync.WaitGroup // running handlers
}

// New creates a Client over pool. It validates all options and returns the
// first error found.
func New(pool *pgxpool.Pool, opts ...Option) (*Client, error) {
	if pool == nil {
		return nil, errors.New("notasap: New: pool is nil")
	}
	c, err := newClient(store.New(pool), pool, opts...)
	if err != nil {
		return nil, err
	}
	c.maxConns = pool.Config().MaxConns
	return c, nil
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
		stopped: make(chan struct{}),
		kinds:   make(map[Kind]*kindEntry),
		names:   make(map[string]Kind),
	}, nil
}

// Start checks that the jobs table is reachable and starts the worker pools
// and maintenance loops. It does not block; ctx bounds only the startup check.
// A Client can be started once: a second call returns ErrAlreadyStarted, a
// call after Stop returns ErrStopped. If the check fails, Start may be retried.
// The client lock is held during the check, blocking Stop, so pass a ctx with
// a deadline.
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
	c.pools = make(map[Kind]*kindPool, len(c.kinds))
	for k, e := range c.kinds {
		p := &kindPool{
			c:      c,
			entry:  e,
			slots:  make(chan struct{}, e.cfg.Workers),
			wake:   make(chan struct{}, 1),
			notify: make(chan struct{}, 1),
		}
		c.pools[k] = p
		c.bgWG.Go(p.loop)
		names = append(names, e.cfg.Name)
	}
	c.startMaintenance()

	slices.Sort(names)
	if c.maxConns > 0 && c.maxConns < int32(len(names)+2) {
		c.logger.Warn("notasap: connection pool is smaller than kinds + 2; status writes may starve, give notasap its own pool with MaxConns >= kinds + 2",
			"max_conns", c.maxConns, "kinds", len(names))
	}
	c.logger.Info("notasap: started", "kinds", names)
	return nil
}

// startMaintenance starts the background maintenance loops in c.bgWG.
func (c *Client) startMaintenance() {
	c.bgWG.Go(c.rescueLoop)
	if !c.janitor.Disabled {
		c.bgWG.Go(c.janitorLoop)
	}
}

// Stop stops polling and maintenance and waits for running handlers until ctx
// is done. Then it cancels handler contexts, waits up to the stop grace for
// them to record their status and returns ctx.Err(). Handlers that ignore
// their context may still be running after Stop returns, and a handler that
// has returned may still retry its status write for up to 10s (dbCallTimeout);
// keep the pool open that long, or the write fails and the rescuer recovers
// the job. When Stop returns nil, all status writes are done. Stop on a Client that was never started returns
// nil. A later Stop waits until the first one returns, then returns nil, or
// returns ctx.Err() if ctx is done first.
func (c *Client) Stop(ctx context.Context) error {
	c.mu.Lock()
	prev := c.state
	c.state = stateStopped
	if prev == stateNew {
		close(c.stopped)
	}
	c.mu.Unlock()
	switch prev {
	case stateNew:
		return nil
	case stateStopped:
		select {
		case <-c.stopped:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer close(c.stopped)

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
	c.cancelRun(errShutdown)
	var err error
	select {
	case <-drained: // drained wins, even if ctx is done too
	default:
		err = ctx.Err()
		grace := time.NewTimer(c.stopGrace)
		defer grace.Stop()
		select {
		case <-drained:
		case <-grace.C:
			c.logger.Warn("notasap: handlers still running after stop grace")
		}
	}
	c.logger.Info("notasap: stopped")
	return err
}
