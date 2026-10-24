package notasap

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstant(t *testing.T) {
	b := Constant(3 * time.Second)
	require.Equal(t, 3*time.Second, b(1))
	require.Equal(t, 3*time.Second, b(50))
}

func TestExponentialBounds(t *testing.T) {
	base, maxD := 100*time.Millisecond, 5*time.Second
	b := Exponential(base, maxD)
	for attempt := 1; attempt <= 10; attempt++ {
		d := min(maxD, base<<(attempt-1))
		for range 200 {
			got := b(attempt)
			require.GreaterOrEqual(t, got, d/2, "attempt %d", attempt)
			require.LessOrEqual(t, got, d, "attempt %d", attempt)
		}
	}
}

func TestExponentialNoOverflow(t *testing.T) {
	maxD := 5 * time.Second
	b := Exponential(100*time.Millisecond, maxD)
	for _, attempt := range []int{1000, math.MaxInt} {
		got := b(attempt)
		require.GreaterOrEqual(t, got, maxD/2)
		require.LessOrEqual(t, got, maxD)
	}
}

func TestExponentialJitterVaries(t *testing.T) {
	b := Exponential(100*time.Millisecond, 5*time.Second)
	seen := map[time.Duration]struct{}{}
	for range 100 {
		seen[b(3)] = struct{}{}
	}
	require.Greater(t, len(seen), 10)
}

func TestExponentialInvalidArgsPanic(t *testing.T) {
	require.Panics(t, func() { Exponential(0, time.Second) })
	require.Panics(t, func() { Exponential(2*time.Second, time.Second) })
	require.Panics(t, func() { Constant(-1) })
}
