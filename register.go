package notasap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/nottolstybai/not-asap/internal/store"
)

// Handler processes a job of payload type T.
type Handler[T Payload] func(ctx context.Context, job *Job[T]) error

// Job is a claimed job passed to a Handler.
type Job[T Payload] struct {
	ID          int64
	Kind        Kind
	Payload     T
	Attempt     int
	MaxAttempts int
	CreatedAt   time.Time
}

type kindEntry struct {
	kind Kind
	cfg  KindConfig
	run  func(ctx context.Context, row store.ClaimedJob) execResult
}

// Register binds handler h to the Kind of payload type T. The kind config is
// merged over the client defaults and validated. It must be called before Start.
func Register[T Payload](c *Client, h Handler[T], cfg KindConfig) error {
	if h == nil {
		return errors.New("notasap: Register: handler is nil")
	}

	if k := reflect.TypeFor[T]().Kind(); k == reflect.Pointer || k == reflect.Interface {
		return fmt.Errorf("notasap: Register: payload type %s must be a non-pointer, non-interface type", reflect.TypeFor[T]())
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.state {
	case stateStarted:
		return ErrAlreadyStarted
	case stateStopped:
		return ErrStopped
	}

	var zero T
	kind := zero.Kind()
	if kind <= 0 {
		return fmt.Errorf("notasap: Register: kind %d must be positive", kind)
	}

	if existing, ok := c.kinds[kind]; ok {
		return fmt.Errorf("notasap: Register: kind %d is already registered as %q", kind, existing.cfg.Name)
	}

	merged := mergeKindConfig(c.defaults, cfg)
	if err := validateKindConfig(merged); err != nil {
		return err
	}

	if other, ok := c.names[merged.Name]; ok {
		return fmt.Errorf("notasap: Register: name %q is already used by kind %d", merged.Name, other)
	}

	c.kinds[kind] = &kindEntry{
		kind: kind,
		cfg:  merged,
		run: func(ctx context.Context, row store.ClaimedJob) execResult {
			var p T
			if err := json.Unmarshal(row.Payload, &p); err != nil {
				return execResult{decodeErr: err}
			}

			return execResult{err: h(ctx, &Job[T]{
				ID:          row.ID,
				Kind:        kind,
				Payload:     p,
				Attempt:     row.Attempt,
				MaxAttempts: row.MaxAttempts,
				CreatedAt:   row.CreatedAt,
			})}
		},
	}
	c.names[merged.Name] = kind

	return nil
}

// entry returns the registered entry for k.
func (c *Client) entry(k Kind) (*kindEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.kinds[k]

	return e, ok
}
