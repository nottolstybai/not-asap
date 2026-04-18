package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// JobArgs is the constraint for payload types. Implementing Kind() string links
// the Go type to the job_type column in notasap_jobs. (REG-02)
type JobArgs interface {
	Kind() string
}

// HandlerFunc is the typed handler signature. T must implement JobArgs.
// The handler receives a context and the fully-unmarshalled args struct. (REG-01)
type HandlerFunc[T JobArgs] func(ctx context.Context, args T) error

// handlerEntry is the type-erased internal representation stored in the registry.
// The handle closure unmarshals raw JSONB bytes into T and calls the typed handler.
type handlerEntry struct {
	taskConfig TaskConfig
	handle     func(ctx context.Context, rawPayload []byte) error
}

// Registry holds registered handlers under a read-write mutex.
// Plain map + sync.RWMutex is preferred over sync.Map for type safety and clarity.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]handlerEntry
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]handlerEntry)}
}

// lookup returns the handlerEntry for kind and true if registered, or zero value and false.
func (r *Registry) lookup(kind string) (handlerEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.handlers[kind]
	return entry, ok
}

// Lookup is the exported version of lookup — returns TaskConfig, handler closure, and bool.
func (r *Registry) Lookup(kind string) (TaskConfig, func(context.Context, []byte) error, bool) {
	entry, ok := r.lookup(kind)
	if !ok {
		return TaskConfig{}, nil, false
	}
	return entry.taskConfig, entry.handle, true
}

// Register registers a typed handler for job kind T.Kind().
//
// This is a package-level generic function — Go does not allow type parameters on methods.
// The closure captures T and calls json.Unmarshal(rawPayload, &args) before invoking handler,
// so the handler always receives a fully-typed T value. (REG-01)
//
// opts are applied to build a TaskConfig for per-type configuration overrides. (CFG-02)
//
// Example:
//
//	type EmailJob struct { To string; Subject string }
//	func (e EmailJob) Kind() string { return "email" }
//
//	core.Register(r, func(ctx context.Context, job EmailJob) error {
//	    return sendEmail(job.To, job.Subject)
//	})
func Register[T JobArgs](r *Registry, handler HandlerFunc[T], opts ...func(*TaskConfig)) {
	var zero T
	kind := zero.Kind()

	tc := TaskConfig{}
	for _, o := range opts {
		o(&tc)
	}

	entry := handlerEntry{
		taskConfig: tc,
		handle: func(ctx context.Context, rawPayload []byte) error {
			var args T
			if err := json.Unmarshal(rawPayload, &args); err != nil {
				return fmt.Errorf("notasap: unmarshal payload for kind %q: %w", kind, err)
			}
			return handler(ctx, args)
		},
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[kind] = entry
}
