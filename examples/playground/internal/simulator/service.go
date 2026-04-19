package simulator

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"sync"
	"time"
)

type Knob struct {
	FailureRate  float64 // 0..1
	LatencyMinMs int
	LatencyMaxMs int
}

type Service struct {
	mu    sync.RWMutex
	knobs map[string]Knob
}

func New(initial map[string]Knob) *Service {
	k := make(map[string]Knob, len(initial))
	maps.Copy(k, initial)
	return &Service{knobs: k}
}

func (s *Service) knob(kind string) Knob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.knobs[kind]
}

// Call sleeps a random duration inside [LatencyMinMs, LatencyMaxMs]
// and returns a synthetic error with probability FailureRate.
// It respects ctx.Done() while sleeping.
func (s *Service) Call(ctx context.Context, kind string) error {
	k := s.knob(kind)

	dur := latency(k)
	if dur > 0 {
		t := time.NewTimer(dur)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}

	if k.FailureRate > 0 && rand.Float64() < k.FailureRate {
		return fmt.Errorf("simulated failure: %s", kind)
	}
	return nil
}

func (s *Service) Update(kind string, k Knob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.knobs[kind] = k
}

func (s *Service) Snapshot() map[string]Knob {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Knob, len(s.knobs))
	maps.Copy(out, s.knobs)
	return out
}

func latency(k Knob) time.Duration {
	if k.LatencyMaxMs <= 0 {
		return 0
	}
	lo := k.LatencyMinMs
	hi := k.LatencyMaxMs
	if lo > hi {
		lo = hi
	}
	ms := lo
	if hi > lo {
		ms = lo + rand.IntN(hi-lo+1)
	}
	return time.Duration(ms) * time.Millisecond
}
