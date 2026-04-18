package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/core"
)

func TestCalculateRetryDelay_Static(t *testing.T) {
	delay := calculateRetryDelay(core.RetryStrategyStatic, 5*time.Second, 1, 10*time.Minute)
	require.Equal(t, 5*time.Second, delay)
}

func TestCalculateRetryDelay_Exponential_Bounds(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := 5 * time.Second

	for attempt := 1; attempt <= 5; attempt++ {
		cap := min(base*time.Duration(1<<(attempt-1)), maxDelay)
		for range 100 {
			delay := calculateRetryDelay(core.RetryStrategyExponential, base, attempt, maxDelay)
			assert.GreaterOrEqual(t, delay, time.Duration(0))
			assert.LessOrEqual(t, delay, cap)
		}
	}
}

func TestCalculateRetryDelay_Exponential_NeverExceedsMax(t *testing.T) {
	maxDelay := 1 * time.Second
	for range 1000 {
		delay := calculateRetryDelay(core.RetryStrategyExponential, 100*time.Millisecond, 20, maxDelay)
		assert.LessOrEqual(t, delay, maxDelay, "attempt=%d", 20)
	}
}

func TestCalculateRetryDelay_ProducesVaryingResults(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := 5 * time.Second
	attempt := 3

	values := make(map[time.Duration]int)
	for range 100 {
		delay := calculateRetryDelay(core.RetryStrategyExponential, base, attempt, maxDelay)
		values[delay]++
	}
	assert.Greater(t, len(values), 10, "jitter should produce varying results")
}

func TestCalculateRetryDelay_UnknownStrategy(t *testing.T) {
	delay := calculateRetryDelay(core.RetryStrategy(999), 5*time.Second, 1, 10*time.Minute)
	require.Equal(t, 5*time.Second, delay)
}
