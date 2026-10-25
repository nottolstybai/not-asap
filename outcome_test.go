package notasap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/nottolstybai/not-asap/internal/pgtest"
	"github.com/nottolstybai/not-asap/internal/store"
)

// defaultTestBackoff is the backoff of TestDecide cases that set none, so
// expected delays of time.Minute come from it.
var defaultTestBackoff = Constant(time.Minute)

func TestDecide(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	tests := []struct {
		name                 string
		res                  execResult
		attempt, maxAttempts int
		backoff              Backoff
		want                 action
	}{
		{name: "nil is done", res: execResult{}, attempt: 1, maxAttempts: 3, want: action{status: StatusDone}},
		{
			name: "cancel", attempt: 1, maxAttempts: 3,
			res:  execResult{err: fmt.Errorf("gone: %w", ErrCancel)},
			want: action{status: StatusCancelled, errMsg: "gone: notasap: cancel"},
		},
		{
			name: "keep attempt", attempt: 2, maxAttempts: 3,
			res:  execResult{err: fmt.Errorf("later: %w", ErrKeepAttempt)},
			want: action{status: StatusPending, delay: time.Minute, refund: true, errMsg: "later: notasap: keep attempt", reason: RetryKeepAttempt},
		},
		{
			name: "error retries", attempt: 1, maxAttempts: 3,
			res:  execResult{err: boom},
			want: action{status: StatusPending, delay: time.Minute, errMsg: "boom", reason: RetryError},
		},
		{
			name: "error exhausted", attempt: 3, maxAttempts: 3,
			res:  execResult{err: boom},
			want: action{status: StatusFailed, errMsg: "boom", reason: RetryError},
		},
		{
			name: "panic retries", attempt: 1, maxAttempts: 3,
			res:  execResult{panicked: true, panicValue: "boom"},
			want: action{status: StatusPending, delay: time.Minute, errMsg: "panic: boom", reason: RetryPanic},
		},
		{
			name: "panic exhausted", attempt: 3, maxAttempts: 3,
			res:  execResult{panicked: true, panicValue: "boom"},
			want: action{status: StatusFailed, errMsg: "panic: boom", reason: RetryPanic},
		},
		{
			name: "timeout", attempt: 1, maxAttempts: 3,
			res:  execResult{err: errors.New("ctx"), cause: errTimeout},
			want: action{status: StatusPending, delay: time.Minute, errMsg: "ctx", reason: RetryTimeout},
		},
		{
			name: "shutdown with error", attempt: 1, maxAttempts: 3,
			res:  execResult{err: boom, cause: errShutdown},
			want: action{status: StatusPending, refund: true, errMsg: "boom", reason: RetryShutdown},
		},
		{name: "shutdown with nil", res: execResult{cause: errShutdown}, attempt: 1, maxAttempts: 3, want: action{status: StatusDone}},
		{
			name: "shutdown with cancel", attempt: 1, maxAttempts: 3,
			res:  execResult{err: ErrCancel, cause: errShutdown},
			want: action{status: StatusCancelled, errMsg: "notasap: cancel"},
		},
		{
			name: "decode error", attempt: 1, maxAttempts: 3,
			res:  execResult{decodeErr: boom},
			want: action{status: StatusFailed, errMsg: "decode payload: boom"},
		},
		{
			name: "negative backoff clamped", attempt: 1, maxAttempts: 3,
			backoff: func(int) time.Duration { return -time.Second },
			res:     execResult{err: boom},
			want:    action{status: StatusPending, errMsg: "boom", reason: RetryError},
		},
		{
			name: "panicking backoff", attempt: 1, maxAttempts: 3,
			backoff: func(int) time.Duration { panic("bad backoff") },
			res:     execResult{err: boom},
			want:    action{status: StatusPending, errMsg: "boom", reason: RetryError},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := tt.backoff
			if b == nil {
				b = defaultTestBackoff
			}
			got := decide(tt.res, tt.attempt, tt.maxAttempts, b, slog.New(slog.DiscardHandler))
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTruncateError(t *testing.T) {
	t.Parallel()
	t.Run("ascii", func(t *testing.T) {
		t.Parallel()
		require.Len(t, truncateError(strings.Repeat("a", 5000)), 4096)
	})
	t.Run("multibyte", func(t *testing.T) {
		t.Parallel()
		// 1 + 2*2047 = 4095 bytes; byte 4096 is mid-rune.
		got := truncateError("a" + strings.Repeat("я", 3000))
		require.True(t, utf8.ValidString(got))
		require.Len(t, got, 4095)
	})
	t.Run("short", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, "short", truncateError("short"))
	})
}

func TestDelayOfRecoversPanic(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	d := delayOf(func(int) time.Duration { panic("bad backoff") }, 2, logger)
	require.Zero(t, d)
	require.Contains(t, buf.String(), "level=ERROR")
	require.Contains(t, buf.String(), "bad backoff")
}

func TestRescueDecisionPanickingBackoff(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	c := newPoolClient(t, newFakeStore(), noop[testPayload], KindConfig{
		Backoff: func(int) time.Duration { panic("bad backoff") },
	}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))

	r := c.rescueDecision(store.StuckJob{
		ID:          1,
		Kind:        1,
		Attempt:     1,
		MaxAttempts: 3,
	})
	require.False(t, r.decision.Fail)
	require.Zero(t, r.decision.Delay)
	require.Contains(t, buf.String(), "level=ERROR")
}

type outcomeSuite struct {
	testSuite
}

func TestOutcome(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(outcomeSuite))
}

func (s *outcomeSuite) Test_kindPool_execute() {
	s.Run("done", func() {
		c := s.startClient([]kindReg{reg(noop[payloadA], KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{N: 1})

		row := pgtest.WaitStatus(&s.Suite, id, StatusDone)
		s.Equal(1, row.Attempt)
		s.Nil(row.Error)
		s.Nil(row.Deadline)
		s.stop(c)
		s.Equal(1, c.obs.Count("finished/done"))
	})

	s.Run("error retries then fails", func() {
		var calls atomic.Int64
		c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
			calls.Add(1)
			return errors.New("boom")
		}, KindConfig{
			Name:        "a",
			MaxAttempts: 3,
		})})
		id := s.schedule(c, payloadA{})

		row := pgtest.WaitStatus(&s.Suite, id, StatusFailed)
		s.Equal(3, row.Attempt)
		s.Require().NotNil(row.Error)
		s.Equal("boom", *row.Error)
		s.EqualValues(3, calls.Load())
	})

	s.Run("panic retried", func() {
		c := s.startClient([]kindReg{reg(func(_ context.Context, j *Job[payloadA]) error {
			if j.Attempt == 1 {
				panic("kaboom")
			}
			return nil
		}, KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})

		row := pgtest.WaitStatus(&s.Suite, id, StatusDone)
		s.Equal(2, row.Attempt)
		s.stop(c)
		s.Equal(1, c.obs.Count("retried/panic"))
	})

	s.Run("timeout retried", func() {
		c := s.startClient([]kindReg{reg(func(ctx context.Context, _ *Job[payloadA]) error {
			<-ctx.Done()
			return ctx.Err()
		}, KindConfig{
			Name:        "a",
			Timeout:     20 * time.Millisecond,
			MaxAttempts: 2,
		})})
		id := s.schedule(c, payloadA{})

		row := pgtest.WaitStatus(&s.Suite, id, StatusFailed)
		s.Equal(2, row.Attempt)
		s.stop(c)
		s.Equal(1, c.obs.Count("retried/timeout"))
		s.Equal(1, c.obs.Count("finished/failed"))
	})

	s.Run("ErrCancel cancels", func() {
		c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
			return fmt.Errorf("give up: %w", ErrCancel)
		}, KindConfig{Name: "a"})})
		id := s.schedule(c, payloadA{})

		row := pgtest.WaitStatus(&s.Suite, id, StatusCancelled)
		s.Require().NotNil(row.Error)
		s.Contains(*row.Error, "cancel")
		s.Equal(1, row.Attempt)
	})

	s.Run("ErrKeepAttempt keeps attempt", func() {
		var calls atomic.Int64
		c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
			if calls.Add(1) <= 3 {
				return fmt.Errorf("not yet: %w", ErrKeepAttempt)
			}
			return nil
		}, KindConfig{
			Name:        "a",
			MaxAttempts: 2,
		})})
		id := s.schedule(c, payloadA{})

		row := pgtest.WaitStatus(&s.Suite, id, StatusDone)
		s.Equal(1, row.Attempt)
		s.stop(c)
		s.Equal(3, c.obs.Count("retried/keep_attempt"))
		s.EqualValues(4, calls.Load())
	})

	s.Run("bad payload fails without handler", func() {
		var called atomic.Bool
		s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
			called.Store(true)
			return nil
		}, KindConfig{Name: "a"})})
		id := s.Fixture.Job(s.ctx, pgtest.JobOpt{
			Kind:    lo.ToPtr(int16(kindA)),
			Payload: lo.ToPtr(`{"n":"str"}`),
		}).ID

		row := pgtest.WaitStatus(&s.Suite, id, StatusFailed)
		s.Equal(1, row.Attempt)
		s.Require().NotNil(row.Error)
		s.True(strings.HasPrefix(*row.Error, "decode payload"), *row.Error)
		s.False(called.Load())
	})

	s.Run("long error stored as valid UTF-8", func() {
		c := s.startClient([]kindReg{reg(func(context.Context, *Job[payloadA]) error {
			return errors.New(strings.Repeat("я", 3000))
		}, KindConfig{
			Name:        "a",
			MaxAttempts: 1,
		})})
		id := s.schedule(c, payloadA{})

		row := pgtest.WaitStatus(&s.Suite, id, StatusFailed)
		s.Require().NotNil(row.Error)
		s.True(utf8.ValidString(*row.Error))
		s.LessOrEqual(len(*row.Error), 4096)
		s.Greater(len(*row.Error), 4000)
	})
}
