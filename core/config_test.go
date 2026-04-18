package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewQueueConfig_Defaults(t *testing.T) {
	cfg, err := NewQueueConfig()
	require.NoError(t, err)
	require.Equal(t, 5, cfg.WorkerCount)
	require.Equal(t, 3, cfg.MaxRetries)
	require.Equal(t, RetryStrategyExponential, cfg.RetryStrategy)
	require.Equal(t, 5*time.Second, cfg.RetryDelay)
	require.Equal(t, 10*time.Minute, cfg.MaxRetryDelay)
	require.Equal(t, 15*time.Second, cfg.HeartbeatInterval)
	require.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
	require.Equal(t, 2*time.Second, cfg.PollInterval)
}

func TestNewQueueConfig_ValidRetryStrategies(t *testing.T) {
	cfg, err := NewQueueConfig(func(c *QueueConfig) {
		c.RetryStrategy = RetryStrategyStatic
	})
	require.NoError(t, err)
	require.Equal(t, RetryStrategyStatic, cfg.RetryStrategy)

	cfg, err = NewQueueConfig(func(c *QueueConfig) {
		c.RetryStrategy = RetryStrategyExponential
	})
	require.NoError(t, err)
	require.Equal(t, RetryStrategyExponential, cfg.RetryStrategy)
}

func TestNewQueueConfig_UnrecognizedRetryStrategy(t *testing.T) {
	_, err := NewQueueConfig(func(c *QueueConfig) {
		c.RetryStrategy = RetryStrategy(99)
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unrecognized RetryStrategy")
}

func TestResolveTaskConfig_Inheritance(t *testing.T) {
	global := QueueConfig{
		MaxRetries:    3,
		RetryDelay:    5 * time.Second,
		RetryStrategy: RetryStrategyExponential,
		MaxRetryDelay: 10 * time.Minute,
	}
	tc := TaskConfig{MaxRetries: 5} // all other fields zero

	eff := ResolveTaskConfig(tc, global)
	require.Equal(t, 5, eff.MaxAttempts)             // overridden by TaskConfig
	require.Equal(t, 5*time.Second, eff.RetryDelay)  // inherited from global
	require.Equal(t, RetryStrategyExponential, eff.RetryStrategy) // inherited from global
	require.Equal(t, 10*time.Minute, eff.MaxRetryDelay) // inherited from global
}

func TestResolveTaskConfig_FullOverride(t *testing.T) {
	global := QueueConfig{
		MaxRetries:    3,
		RetryDelay:    5 * time.Second,
		RetryStrategy: RetryStrategyExponential,
		MaxRetryDelay: 10 * time.Minute,
	}
	tc := TaskConfig{
		MaxRetries:    7,
		RetryDelay:    1 * time.Second,
		RetryStrategy: RetryStrategyStatic,
		MaxRetryDelay: 30 * time.Second,
	}

	eff := ResolveTaskConfig(tc, global)
	require.Equal(t, 7, eff.MaxAttempts)
	require.Equal(t, 1*time.Second, eff.RetryDelay)
	require.Equal(t, RetryStrategyStatic, eff.RetryStrategy)
	require.Equal(t, 30*time.Second, eff.MaxRetryDelay)
}

func TestResolveTaskConfig_Immutability(t *testing.T) {
	global := QueueConfig{
		MaxRetries:    3,
		RetryDelay:    5 * time.Second,
		RetryStrategy: RetryStrategyExponential,
		MaxRetryDelay: 10 * time.Minute,
	}
	tc := TaskConfig{}

	eff := ResolveTaskConfig(tc, global)
	require.Equal(t, 3, eff.MaxAttempts)

	// Mutate global after resolve — EffectiveConfig must be unchanged
	global.MaxRetries = 99
	require.Equal(t, 3, eff.MaxAttempts, "EffectiveConfig must be immune to post-resolve mutation")
}

func TestJanitorConfig_RoundTrip(t *testing.T) {
	jc := JanitorConfig{
		TTL:         24 * time.Hour,
		Statuses:    []Status{StatusDone, StatusExhausted},
		RunInterval: 10 * time.Minute,
		Disabled:    false,
	}
	require.Equal(t, 24*time.Hour, jc.TTL)
	require.Len(t, jc.Statuses, 2)
	require.Equal(t, StatusDone, jc.Statuses[0])
	require.Equal(t, StatusExhausted, jc.Statuses[1])
	require.Equal(t, 10*time.Minute, jc.RunInterval)
	require.False(t, jc.Disabled)
}
