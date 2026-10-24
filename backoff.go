package notasap

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the delay before retrying after the given failed attempt (1-based).
type Backoff func(attempt int) time.Duration

// Constant returns a Backoff that always yields d.
// It panics if d is negative; call it at setup time, like regexp.MustCompile.
func Constant(d time.Duration) Backoff {
	if d < 0 {
		panic("notasap: Constant: negative duration")
	}
	return func(int) time.Duration { return d }
}

// Exponential returns a Backoff doubling from base up to max, with jitter
// in [d/2, d]. It is overflow-safe for any attempt number.
// It panics unless 0 < base <= max; call it at setup time, like regexp.MustCompile.
func Exponential(base, max time.Duration) Backoff {
	if base <= 0 {
		panic("notasap: Exponential: base must be positive")
	}
	if max < base {
		panic("notasap: Exponential: max must be >= base")
	}
	return func(attempt int) time.Duration {
		d := base
		for shift := 1; shift < attempt; shift++ {
			if d > max/2 || shift >= 62 {
				d = max
				break
			}
			d *= 2
		}
		return d/2 + rand.N(d/2+1)
	}
}
