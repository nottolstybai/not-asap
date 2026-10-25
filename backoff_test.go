package notasap

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstant(t *testing.T) {
	t.Parallel()
	b := Constant(3 * time.Second)
	require.Equal(t, 3*time.Second, b(1))
	require.Equal(t, 3*time.Second, b(50))
	require.Panics(t, func() { Constant(-1) })
}

func TestExponential(t *testing.T) {
	t.Parallel()
	t.Run("bounds", func(t *testing.T) {
		t.Parallel()
		base, maxD := 100*time.Millisecond, 5*time.Second
		b := Exponential(base, maxD)
		for attempt := 1; attempt <= 10; attempt++ {
			t.Run(fmt.Sprint(attempt), func(t *testing.T) {
				t.Parallel()
				d := min(maxD, base<<(attempt-1))
				for range 200 {
					got := b(attempt)
					require.GreaterOrEqual(t, got, d/2)
					require.LessOrEqual(t, got, d)
				}
			})
		}
	})

	t.Run("no overflow", func(t *testing.T) {
		t.Parallel()
		maxD := 5 * time.Second
		b := Exponential(100*time.Millisecond, maxD)
		for _, attempt := range []int{1000, math.MaxInt} {
			t.Run(fmt.Sprint(attempt), func(t *testing.T) {
				t.Parallel()
				got := b(attempt)
				require.GreaterOrEqual(t, got, maxD/2)
				require.LessOrEqual(t, got, maxD)
			})
		}
	})

	t.Run("jitter varies", func(t *testing.T) {
		t.Parallel()
		b := Exponential(100*time.Millisecond, 5*time.Second)
		seen := map[time.Duration]struct{}{}
		for range 100 {
			seen[b(3)] = struct{}{}
		}
		require.Greater(t, len(seen), 10)
	})

	t.Run("invalid args panic", func(t *testing.T) {
		t.Parallel()
		require.Panics(t, func() { Exponential(0, time.Second) })
		require.Panics(t, func() { Exponential(2*time.Second, time.Second) })
	})
}
