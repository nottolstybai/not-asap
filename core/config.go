package core

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RetryStrategy controls how retry delays are computed.
// Zero value means "unset — inherit from parent config".
// CRITICAL: iota+1 so zero value != any valid strategy (enables CFG-02 inheritance).
type RetryStrategy int

const (
	RetryStrategyStatic      RetryStrategy = iota + 1 // fixed RetryDelay each attempt
	RetryStrategyExponential                           // exponential backoff with jitter
)

// QueueConfig holds global settings for a Queue instance. (CFG-01)
// All fields have sensible defaults via defaultQueueConfig().
type QueueConfig struct {
	WorkerCount       int
	PollInterval      time.Duration
	MaxRetries        int
	RetryDelay        time.Duration
	RetryStrategy     RetryStrategy
	MaxRetryDelay     time.Duration
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
	Logger            *slog.Logger          // nil -> slog.Default() used at Queue start
	Registerer        prometheus.Registerer // nil -> metrics disabled; never use prometheus.DefaultRegisterer
	Janitor           JanitorConfig         // automatic cleanup of terminal jobs
}

// TaskConfig holds per-job-type overrides. Zero-value fields inherit from QueueConfig. (CFG-02)
type TaskConfig struct {
	MaxRetries    int
	RetryDelay    time.Duration
	RetryStrategy RetryStrategy
	MaxRetryDelay time.Duration
}

// JanitorConfig controls automatic cleanup of terminal jobs. (CFG-04)
type JanitorConfig struct {
	TTL         time.Duration // age of updated_at after which terminal rows are deleted
	Statuses    []Status      // which terminal statuses to clean; default: done, exhausted, cancelled
	RunInterval time.Duration // how often to run the janitor
	Disabled    bool          // set true to suppress all cleanup
}

// EffectiveConfig is the resolved per-job configuration used at dispatch/insert time.
// It is always fully populated (no zero-value inheritance fields).
type EffectiveConfig struct {
	MaxAttempts   int
	RetryDelay    time.Duration
	RetryStrategy RetryStrategy
	MaxRetryDelay time.Duration
}

// defaultQueueConfig returns QueueConfig with conservative production-safe defaults.
func defaultQueueConfig() QueueConfig {
	return QueueConfig{
		WorkerCount:       5,
		PollInterval:      2 * time.Second,
		MaxRetries:        3,
		RetryDelay:        5 * time.Second,
		RetryStrategy:     RetryStrategyExponential,
		MaxRetryDelay:     10 * time.Minute,
		HeartbeatInterval: 15 * time.Second,
		ShutdownTimeout:   30 * time.Second,
		Janitor: JanitorConfig{
			TTL:         24 * time.Hour,
			Statuses:    []Status{StatusDone, StatusExhausted, StatusCancelled},
			RunInterval: 1 * time.Hour,
			Disabled:    false,
		},
	}
}

// NewQueueConfig applies options on top of defaults and validates the result.
// Returns a non-nil error if RetryStrategy is set to an unrecognized value. (CFG-03)
func NewQueueConfig(opts ...func(*QueueConfig)) (QueueConfig, error) {
	cfg := defaultQueueConfig()
	for _, o := range opts {
		o(&cfg)
	}
	switch cfg.RetryStrategy {
	case RetryStrategyStatic, RetryStrategyExponential:
		// valid
	default:
		return QueueConfig{}, fmt.Errorf("notasap: unrecognized RetryStrategy %d; use RetryStrategyStatic or RetryStrategyExponential", cfg.RetryStrategy)
	}
	return cfg, nil
}

// ResolveTaskConfig merges per-type TaskConfig on top of global QueueConfig.
// Any zero-value field in tc is replaced by the corresponding global default. (CFG-02, LIFE-05)
// This is a pure function; the result is a value type (copy) — callers cannot mutate the source.
func ResolveTaskConfig(tc TaskConfig, global QueueConfig) EffectiveConfig {
	eff := EffectiveConfig{
		MaxAttempts:   global.MaxRetries,
		RetryDelay:    global.RetryDelay,
		RetryStrategy: global.RetryStrategy,
		MaxRetryDelay: global.MaxRetryDelay,
	}
	if tc.MaxRetries != 0 {
		eff.MaxAttempts = tc.MaxRetries
	}
	if tc.RetryDelay != 0 {
		eff.RetryDelay = tc.RetryDelay
	}
	if tc.RetryStrategy != 0 {
		eff.RetryStrategy = tc.RetryStrategy
	}
	if tc.MaxRetryDelay != 0 {
		eff.MaxRetryDelay = tc.MaxRetryDelay
	}
	return eff
}
