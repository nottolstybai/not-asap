package notasap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/nottolstybai/not-asap/internal/store"
)

type scheduleOptions struct {
	delay   time.Duration
	key     *string
	replace bool
	err     error
}

// ScheduleOption customizes a scheduled job.
type ScheduleOption func(*scheduleOptions)

// WithDelay sets run_at to the database NOW() plus d. d must not be negative.
func WithDelay(d time.Duration) ScheduleOption {
	return func(o *scheduleOptions) {
		if d < 0 {
			o.err = errors.New("notasap: WithDelay: delay must not be negative")
			return
		}
		o.delay = d
	}
}

// WithIdempotencyKey sets the idempotency key: while a job of the same Kind
// with this key exists, scheduling another one returns ErrDuplicate.
func WithIdempotencyKey(key string) ScheduleOption {
	return func(o *scheduleOptions) {
		if key == "" {
			o.err = errors.New("notasap: WithIdempotencyKey: key is empty")
			return
		}
		o.key = &key
	}
}

// WithReplace overwrites an existing non-running job with the same idempotency
// key instead of returning ErrDuplicate. It requires WithIdempotencyKey.
// A running job is left as is and ErrJobRunning is returned.
func WithReplace() ScheduleOption {
	return func(o *scheduleOptions) { o.replace = true }
}

// BatchItem is one job of ScheduleBatch.
type BatchItem struct {
	payload Payload
	opts    []ScheduleOption
}

// Item builds a BatchItem.
func Item(p Payload, opts ...ScheduleOption) BatchItem {
	return BatchItem{payload: p, opts: opts}
}

// BatchResult is the outcome of one BatchItem. Err is nil, ErrDuplicate or
// ErrJobRunning; ID is the new or existing job.
type BatchResult struct {
	ID  int64
	Err error
}

// Schedule inserts a job for p. On an idempotency key conflict it returns the
// existing job ID with ErrDuplicate or ErrJobRunning.
func (c *Client) Schedule(ctx context.Context, p Payload, opts ...ScheduleOption) (int64, error) {
	return c.scheduleOne(ctx, c.db, p, opts, true)
}

// ScheduleTx is Schedule within tx. The job becomes visible when tx commits.
// The observer is notified before the commit. A keyed schedule holds its key
// until tx ends, so concurrent schedules of that key wait for tx. Several
// keyed schedules in one tx must be issued in a consistent (kind, key) order
// across the application, or Postgres may abort tx with a deadlock (SQLSTATE
// 40P01); retry tx on 40P01.
func (c *Client) ScheduleTx(ctx context.Context, tx pgx.Tx, p Payload, opts ...ScheduleOption) (int64, error) {
	if tx == nil {
		return 0, errors.New("notasap: ScheduleTx: tx is nil")
	}
	return c.scheduleOne(ctx, tx, p, opts, false)
}

// ScheduleBatch inserts all items atomically. Per-item idempotency conflicts
// are reported in the results; any other error fails the whole batch and
// nothing is written. A batch without idempotency keys takes one round trip;
// one with keys runs in its own transaction, as resolving conflicts may need
// more round trips.
func (c *Client) ScheduleBatch(ctx context.Context, items ...BatchItem) ([]BatchResult, error) {
	return c.scheduleBatch(ctx, c.db, items, true)
}

// ScheduleBatchTx is ScheduleBatch within tx. On a database error tx is
// aborted; validation errors send no SQL and leave tx usable. Items are
// inserted in (kind, key) order, but keys scheduled by other calls in the
// same tx must follow a consistent (kind, key) order across the application,
// or Postgres may abort tx with a deadlock (SQLSTATE 40P01); retry tx on 40P01.
func (c *Client) ScheduleBatchTx(ctx context.Context, tx pgx.Tx, items ...BatchItem) ([]BatchResult, error) {
	if tx == nil {
		return nil, errors.New("notasap: ScheduleBatchTx: tx is nil")
	}
	return c.scheduleBatch(ctx, tx, items, false)
}

func (c *Client) scheduleOne(ctx context.Context, db store.DB, p Payload, opts []ScheduleOption, ownTx bool) (int64, error) {
	res, err := c.scheduleBatch(ctx, db, []BatchItem{{payload: p, opts: opts}}, ownTx)
	if err != nil {
		return 0, err
	}
	return res[0].ID, res[0].Err
}

// scheduleBatch validates items and inserts them. ownTx means db is not the
// caller's tx: a batch of several items with any idempotency key runs in a
// transaction on db, so the extra conflict-resolution round trips cannot leave
// it partially committed, and the inserts are committed on return, so the
// pollers of jobs that are ready now are woken.
func (c *Client) scheduleBatch(ctx context.Context, db store.DB, items []BatchItem, ownTx bool) ([]BatchResult, error) {
	if len(items) == 0 {
		return nil, nil
	}
	params := make([]store.InsertParams, len(items))
	names := make([]string, len(items))
	for i, it := range items {
		p, name, err := c.insertParams(it)
		if err != nil {
			return nil, err
		}
		params[i] = p
		names[i] = name
	}

	var inserted []store.InsertResult
	insert := func(db store.DB) error {
		var err error
		inserted, err = c.st.Insert(ctx, db, params)
		return err
	}
	var err error
	if ownTx && len(params) > 1 && slices.ContainsFunc(params, func(p store.InsertParams) bool { return p.Key != nil }) {
		err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error { return insert(tx) })
	} else {
		err = insert(db)
	}
	if err != nil {
		return nil, err
	}

	res := make([]BatchResult, len(items))
	var ready []Kind
	for i, r := range inserted {
		res[i].ID = r.ID
		switch r.Outcome {
		case store.Inserted, store.Replaced:
			c.observer.JobScheduled(names[i])
			if ownTx && params[i].Delay == 0 {
				ready = append(ready, Kind(params[i].Kind))
			}
		case store.Duplicate:
			res[i].Err = ErrDuplicate
		case store.Running:
			res[i].Err = ErrJobRunning
		}
	}
	c.notify(ready)
	return res, nil
}

// notify wakes the idle pollers of kinds without blocking. Before Start it
// does nothing; after Stop nobody receives the signal.
func (c *Client) notify(kinds []Kind) {
	if len(kinds) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range kinds {
		if p := c.pools[k]; p != nil {
			select {
			case p.notify <- struct{}{}:
			default:
			}
		}
	}
}

// insertParams validates it and returns its insert parameters and kind name.
func (c *Client) insertParams(it BatchItem) (store.InsertParams, string, error) {
	if it.payload == nil {
		return store.InsertParams{}, "", errors.New("notasap: schedule: payload is nil")
	}
	kind := it.payload.Kind()
	e, ok := c.entry(kind)
	if !ok {
		return store.InsertParams{}, "", fmt.Errorf("%w %d", ErrUnknownKind, kind)
	}
	var o scheduleOptions
	for _, opt := range it.opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.err != nil {
		return store.InsertParams{}, "", o.err
	}
	if o.replace && o.key == nil {
		return store.InsertParams{}, "", errors.New("notasap: schedule: WithReplace requires WithIdempotencyKey")
	}
	payload, err := json.Marshal(it.payload)
	if err != nil {
		return store.InsertParams{}, "", fmt.Errorf("notasap: schedule: marshal payload of kind %d: %w", kind, err)
	}
	return store.InsertParams{
		Kind:        int16(kind),
		Payload:     payload,
		MaxAttempts: e.cfg.MaxAttempts,
		Delay:       o.delay,
		Key:         o.key,
		Replace:     o.replace,
	}, e.cfg.Name, nil
}
