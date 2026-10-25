package notasap

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestNewValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		opt  Option
		want string
	}{
		{"nil logger", WithLogger(nil), "WithLogger"},
		{"nil observer", WithObserver(nil), "WithObserver"},
		{"defaults name", WithDefaults(KindConfig{Name: "x"}), "Name"},
		{"defaults workers", WithDefaults(KindConfig{Workers: -1}), "Workers"},
		{"defaults attempts", WithDefaults(KindConfig{MaxAttempts: -1}), "MaxAttempts"},
		{"defaults timeout", WithDefaults(KindConfig{Timeout: -1}), "Timeout"},
		{"defaults poll", WithDefaults(KindConfig{PollInterval: -1}), "PollInterval"},
		{"rescuer interval", WithRescuer(RescuerConfig{Interval: -1}), "Interval"},
		{"rescuer grace", WithRescuer(RescuerConfig{Grace: -1}), "Grace"},
		{"janitor interval", WithJanitor(JanitorConfig{Interval: -1}), "Interval"},
		{"janitor batch", WithJanitor(JanitorConfig{BatchSize: -1}), "BatchSize"},
		{"janitor retention key", WithJanitor(JanitorConfig{Retention: map[Status]time.Duration{StatusPending: time.Hour}}), "Retention"},
		{"janitor retention negative", WithJanitor(JanitorConfig{Retention: map[Status]time.Duration{StatusDone: -1}}), "Retention"},
		{"stop grace", WithStopGrace(-1), "WithStopGrace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := newClient(nil, nil, tt.opt)
			require.ErrorContains(t, err, tt.want)
		})
	}

	t.Run("nil pool", func(t *testing.T) {
		t.Parallel()
		_, err := New(nil)
		require.ErrorContains(t, err, "pool")
	})

	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		c, err := newClient(nil, nil,
			WithLogger(slog.Default()),
			WithObserver(NopObserver{}),
			WithDefaults(KindConfig{Workers: 2, Backoff: Constant(time.Second)}),
			WithRescuer(RescuerConfig{Interval: time.Second, Grace: time.Second}),
			WithJanitor(JanitorConfig{Interval: time.Minute, BatchSize: 10, Retention: map[Status]time.Duration{StatusDone: time.Hour}}),
			WithStopGrace(time.Second),
		)
		require.NoError(t, err)
		require.Equal(t, 2, c.defaults.Workers)
		require.Equal(t, stateNew, c.state)
	})
}

func TestMergeKindConfig(t *testing.T) {
	t.Parallel()

	c, err := newClient(nil, nil)
	require.NoError(t, err)
	got := mergeKindConfig(c.defaults, KindConfig{Name: "x"})
	require.Equal(t, 5, got.Workers)
	got = mergeKindConfig(c.defaults, KindConfig{Name: "x", Workers: 7})
	require.Equal(t, 7, got.Workers)
	require.Equal(t, 3, got.MaxAttempts)
	require.Equal(t, time.Minute, got.Timeout)
	require.Equal(t, time.Second, got.PollInterval)
	require.NotNil(t, got.Backoff)

	c, err = newClient(nil, nil, WithDefaults(KindConfig{MaxAttempts: 9}))
	require.NoError(t, err)
	got = mergeKindConfig(c.defaults, KindConfig{Name: "x"})
	require.Equal(t, 9, got.MaxAttempts)
	require.Equal(t, 5, got.Workers)
}

func TestValidateKindConfig(t *testing.T) {
	t.Parallel()
	valid := KindConfig{Name: "x", Workers: 1, MaxAttempts: 1, Timeout: time.Second, PollInterval: time.Second, Backoff: Constant(0)}
	require.NoError(t, validateKindConfig(valid))

	tests := []struct {
		name   string
		mutate func(*KindConfig)
		want   string
	}{
		{"empty name", func(c *KindConfig) { c.Name = "" }, "Name"},
		{"zero workers", func(c *KindConfig) { c.Workers = 0 }, "Workers"},
		{"negative workers", func(c *KindConfig) { c.Workers = -1 }, "Workers"},
		{"zero attempts", func(c *KindConfig) { c.MaxAttempts = 0 }, "MaxAttempts"},
		{"zero timeout", func(c *KindConfig) { c.Timeout = 0 }, "Timeout"},
		{"negative timeout", func(c *KindConfig) { c.Timeout = -1 }, "Timeout"},
		{"zero poll", func(c *KindConfig) { c.PollInterval = 0 }, "PollInterval"},
		{"nil backoff", func(c *KindConfig) { c.Backoff = nil }, "Backoff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid
			tt.mutate(&cfg)
			require.ErrorContains(t, validateKindConfig(cfg), tt.want)
		})
	}
}

func TestJanitorRetentionMerge(t *testing.T) {
	t.Parallel()

	c, err := newClient(nil, nil, WithJanitor(JanitorConfig{Retention: map[Status]time.Duration{StatusDone: time.Hour}}))
	require.NoError(t, err)
	require.Equal(t, map[Status]time.Duration{
		StatusDone:      time.Hour,
		StatusCancelled: 24 * time.Hour,
		StatusFailed:    0,
	}, c.janitor.Retention)

	c, err = newClient(nil, nil, WithJanitor(JanitorConfig{Retention: map[Status]time.Duration{StatusCancelled: 0}}))
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), c.janitor.Retention[StatusCancelled])
	require.Equal(t, 24*time.Hour, c.janitor.Retention[StatusDone])
}

func TestJanitorDisabledMerge(t *testing.T) {
	t.Parallel()

	c, err := newClient(nil, nil)
	require.NoError(t, err)
	require.False(t, c.janitor.Disabled)

	c, err = newClient(nil, nil, WithJanitor(JanitorConfig{Disabled: true}))
	require.NoError(t, err)
	require.True(t, c.janitor.Disabled)

	c, err = newClient(nil, nil,
		WithJanitor(JanitorConfig{Disabled: true}),
		WithJanitor(JanitorConfig{Interval: time.Minute}),
	)
	require.NoError(t, err)
	require.True(t, c.janitor.Disabled)
	require.Equal(t, time.Minute, c.janitor.Interval)
}

func TestNewWiring(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db")
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	c, err := New(pool,
		WithLogger(slog.Default()),
		WithObserver(NopObserver{}),
		WithDefaults(KindConfig{Workers: 2}),
		WithRescuer(RescuerConfig{Interval: time.Second}),
		WithJanitor(JanitorConfig{Interval: time.Minute}),
		WithStopGrace(time.Second),
	)
	require.NoError(t, err)
	require.Equal(t, 2, c.defaults.Workers)
	require.NotNil(t, c.st)
	require.NotNil(t, c.db)
}
