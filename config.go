package notasap

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"
)

const (
	defaultWorkers      = 5
	defaultMaxAttempts  = 3
	defaultTimeout      = time.Minute
	defaultPollInterval = time.Second

	defaultRescuerInterval = 30 * time.Second
	defaultRescuerGrace    = 30 * time.Second

	defaultJanitorInterval = 10 * time.Minute
	defaultJanitorBatch    = 1000

	defaultStopGrace = 5 * time.Second
)

func defaultBackoff() Backoff { return Exponential(5*time.Second, 10*time.Minute) }

func defaultRetention() map[Status]time.Duration {
	return map[Status]time.Duration{
		StatusDone:      24 * time.Hour,
		StatusCancelled: 24 * time.Hour,
		StatusFailed:    0,
	}
}

// KindConfig configures the worker pool of a single Kind. Zero fields fall
// back to WithDefaults and then to built-in defaults.
type KindConfig struct {
	// Name is a human-readable kind name used in logs and metrics. Required at Register.
	Name string
	// Workers is the number of concurrent handlers. Default 5.
	Workers int
	// MaxAttempts is the maximum number of attempts per job. Default 3.
	MaxAttempts int
	// Timeout bounds a single handler run. Default 1m.
	Timeout time.Duration
	// Backoff computes the delay before a retry. Default Exponential(5s, 10m).
	Backoff Backoff
	// PollInterval is how often an idle pool polls for jobs. Default 1s.
	PollInterval time.Duration
}

// RescuerConfig configures the loop returning stuck running jobs to pending.
type RescuerConfig struct {
	// Interval between rescue passes. Default 30s.
	Interval time.Duration
	// Grace is added to a claimed job's deadline (claim + Timeout + Grace); the
	// rescuer picks running jobs whose deadline has passed. Default 30s.
	Grace time.Duration
}

// JanitorConfig configures deletion of finished jobs.
type JanitorConfig struct {
	// Interval between cleanup passes. Default 10m.
	Interval time.Duration
	// BatchSize is the maximum rows deleted per statement. Default 1000.
	BatchSize int
	// Retention is how long jobs in a status are kept; 0 means never delete.
	// Merged per key over defaults (done 24h, cancelled 24h, failed 0).
	Retention map[Status]time.Duration
	// Disabled turns the janitor off.
	Disabled bool
}

type options struct {
	logger    *slog.Logger
	observer  Observer
	defaults  KindConfig
	rescuer   RescuerConfig
	janitor   JanitorConfig
	stopGrace time.Duration
}

func newOptions() options {
	return options{
		logger:   slog.Default(),
		observer: NopObserver{},
		defaults: KindConfig{
			Workers:      defaultWorkers,
			MaxAttempts:  defaultMaxAttempts,
			Timeout:      defaultTimeout,
			Backoff:      defaultBackoff(),
			PollInterval: defaultPollInterval,
		},
		rescuer: RescuerConfig{Interval: defaultRescuerInterval, Grace: defaultRescuerGrace},
		janitor: JanitorConfig{
			Interval:  defaultJanitorInterval,
			BatchSize: defaultJanitorBatch,
			Retention: defaultRetention(),
		},
		stopGrace: defaultStopGrace,
	}
}

// Option customizes a Client.
type Option func(*options) error

// WithLogger sets the logger. It must not be nil.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) error {
		if l == nil {
			return errors.New("notasap: WithLogger: logger is nil")
		}
		o.logger = l
		return nil
	}
}

// WithObserver sets the observer. It must not be nil.
func WithObserver(obs Observer) Option {
	return func(o *options) error {
		if obs == nil {
			return errors.New("notasap: WithObserver: observer is nil")
		}
		o.observer = obs
		return nil
	}
}

// WithDefaults sets client-wide kind defaults; zero fields keep built-in
// defaults. Name must be empty. The merged config is validated per kind at Register.
func WithDefaults(cfg KindConfig) Option {
	return func(o *options) error {
		switch {
		case cfg.Name != "":
			return errors.New("notasap: WithDefaults: Name must be empty")
		case cfg.Workers < 0:
			return errors.New("notasap: WithDefaults: Workers must not be negative")
		case cfg.MaxAttempts < 0:
			return errors.New("notasap: WithDefaults: MaxAttempts must not be negative")
		case cfg.Timeout < 0:
			return errors.New("notasap: WithDefaults: Timeout must not be negative")
		case cfg.PollInterval < 0:
			return errors.New("notasap: WithDefaults: PollInterval must not be negative")
		}
		o.defaults = mergeKindConfig(o.defaults, cfg)
		return nil
	}
}

// WithRescuer configures the rescuer; zero fields keep defaults.
func WithRescuer(cfg RescuerConfig) Option {
	return func(o *options) error {
		switch {
		case cfg.Interval < 0:
			return errors.New("notasap: WithRescuer: Interval must not be negative")
		case cfg.Grace < 0:
			return errors.New("notasap: WithRescuer: Grace must not be negative")
		}
		if cfg.Interval != 0 {
			o.rescuer.Interval = cfg.Interval
		}
		if cfg.Grace != 0 {
			o.rescuer.Grace = cfg.Grace
		}
		return nil
	}
}

// WithJanitor configures the janitor; zero fields keep defaults.
func WithJanitor(cfg JanitorConfig) Option {
	return func(o *options) error {
		switch {
		case cfg.Interval < 0:
			return errors.New("notasap: WithJanitor: Interval must not be negative")
		case cfg.BatchSize < 0:
			return errors.New("notasap: WithJanitor: BatchSize must not be negative")
		}
		for s, d := range cfg.Retention {
			switch s {
			case StatusDone, StatusFailed, StatusCancelled:
			default:
				return fmt.Errorf("notasap: WithJanitor: Retention: unsupported status %s", s)
			}
			if d < 0 {
				return fmt.Errorf("notasap: WithJanitor: Retention[%s] must not be negative", s)
			}
		}
		if cfg.Interval != 0 {
			o.janitor.Interval = cfg.Interval
		}
		if cfg.BatchSize != 0 {
			o.janitor.BatchSize = cfg.BatchSize
		}
		ret := make(map[Status]time.Duration, len(o.janitor.Retention)+len(cfg.Retention))
		maps.Copy(ret, o.janitor.Retention)
		maps.Copy(ret, cfg.Retention)
		o.janitor.Retention = ret
		if cfg.Disabled {
			o.janitor.Disabled = true
		}
		return nil
	}
}

// WithStopGrace sets how long Stop waits, after cancelling handler contexts
// because its ctx expired, for handlers to record their status. Default 5s.
func WithStopGrace(d time.Duration) Option {
	return func(o *options) error {
		if d < 0 {
			return errors.New("notasap: WithStopGrace: duration must not be negative")
		}
		o.stopGrace = d
		return nil
	}
}

// mergeKindConfig fills zero fields of cfg from defaults.
func mergeKindConfig(defaults, cfg KindConfig) KindConfig {
	if cfg.Workers == 0 {
		cfg.Workers = defaults.Workers
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = defaults.MaxAttempts
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaults.Timeout
	}
	if cfg.Backoff == nil {
		cfg.Backoff = defaults.Backoff
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaults.PollInterval
	}
	return cfg
}

// validateKindConfig checks a merged KindConfig.
func validateKindConfig(cfg KindConfig) error {
	switch {
	case cfg.Name == "":
		return errors.New("notasap: KindConfig: Name is empty")
	case cfg.Workers < 1:
		return fmt.Errorf("notasap: KindConfig %q: Workers must be >= 1", cfg.Name)
	case cfg.MaxAttempts < 1:
		return fmt.Errorf("notasap: KindConfig %q: MaxAttempts must be >= 1", cfg.Name)
	case cfg.Timeout <= 0:
		return fmt.Errorf("notasap: KindConfig %q: Timeout must be positive", cfg.Name)
	case cfg.PollInterval <= 0:
		return fmt.Errorf("notasap: KindConfig %q: PollInterval must be positive", cfg.Name)
	case cfg.Backoff == nil:
		return fmt.Errorf("notasap: KindConfig %q: Backoff is nil", cfg.Name)
	}
	return nil
}
