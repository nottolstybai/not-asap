package notasap

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/internal/store"
)

func TestDecide(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name         string
		res          execResult
		attempt, max int
		backoff      Backoff
		want         action
	}{
		{name: "nil is done", res: execResult{}, attempt: 1, max: 3, want: action{status: StatusDone}},
		{
			name: "cancel", attempt: 1, max: 3,
			res:  execResult{err: fmt.Errorf("gone: %w", ErrCancel)},
			want: action{status: StatusCancelled, errMsg: "gone: notasap: cancel"},
		},
		{
			name: "keep attempt", attempt: 2, max: 3,
			res:  execResult{err: fmt.Errorf("later: %w", ErrKeepAttempt)},
			want: action{status: StatusPending, delay: time.Minute, refund: true, errMsg: "later: notasap: keep attempt", reason: RetryKeepAttempt},
		},
		{
			name: "error retries", attempt: 1, max: 3,
			res:  execResult{err: boom},
			want: action{status: StatusPending, delay: time.Minute, errMsg: "boom", reason: RetryError},
		},
		{
			name: "error exhausted", attempt: 3, max: 3,
			res:  execResult{err: boom},
			want: action{status: StatusFailed, errMsg: "boom", reason: RetryError},
		},
		{
			name: "panic retries", attempt: 1, max: 3,
			res:  execResult{panicked: true, panicValue: "boom"},
			want: action{status: StatusPending, delay: time.Minute, errMsg: "panic: boom", reason: RetryPanic},
		},
		{
			name: "panic exhausted", attempt: 3, max: 3,
			res:  execResult{panicked: true, panicValue: "boom"},
			want: action{status: StatusFailed, errMsg: "panic: boom", reason: RetryPanic},
		},
		{
			name: "timeout", attempt: 1, max: 3,
			res:  execResult{err: errors.New("ctx"), cause: errTimeout},
			want: action{status: StatusPending, delay: time.Minute, errMsg: "ctx", reason: RetryTimeout},
		},
		{
			name: "shutdown with error", attempt: 1, max: 3,
			res:  execResult{err: boom, cause: errShutdown},
			want: action{status: StatusPending, refund: true, errMsg: "boom", reason: RetryShutdown},
		},
		{name: "shutdown with nil", res: execResult{cause: errShutdown}, attempt: 1, max: 3, want: action{status: StatusDone}},
		{
			name: "shutdown with cancel", attempt: 1, max: 3,
			res:  execResult{err: ErrCancel, cause: errShutdown},
			want: action{status: StatusCancelled, errMsg: "notasap: cancel"},
		},
		{
			name: "decode error", attempt: 1, max: 3,
			res:  execResult{decodeErr: boom},
			want: action{status: StatusFailed, errMsg: "decode payload: boom"},
		},
		{
			name: "negative backoff clamped", attempt: 1, max: 3,
			backoff: func(int) time.Duration { return -time.Second },
			res:     execResult{err: boom},
			want:    action{status: StatusPending, errMsg: "boom", reason: RetryError},
		},
		{
			name: "panicking backoff", attempt: 1, max: 3,
			backoff: func(int) time.Duration { panic("bad backoff") },
			res:     execResult{err: boom},
			want:    action{status: StatusPending, errMsg: "boom", reason: RetryError},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.backoff
			if b == nil {
				b = Constant(time.Minute)
			}
			got := decide(tt.res, tt.attempt, tt.max, b, slog.New(slog.DiscardHandler))
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTruncateError(t *testing.T) {
	t.Run("ascii", func(t *testing.T) {
		require.Len(t, truncateError(strings.Repeat("a", 5000)), 4096)
	})
	t.Run("multibyte", func(t *testing.T) {
		// 1 + 2*2047 = 4095 bytes; byte 4096 is mid-rune.
		got := truncateError("a" + strings.Repeat("я", 3000))
		require.True(t, utf8.ValidString(got))
		require.Len(t, got, 4095)
	})
	t.Run("short", func(t *testing.T) {
		require.Equal(t, "short", truncateError("short"))
	})
}

func TestDelayOfRecoversPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	d := delayOf(func(int) time.Duration { panic("bad backoff") }, 2, logger)
	require.Zero(t, d)
	require.Contains(t, buf.String(), "level=ERROR")
	require.Contains(t, buf.String(), "bad backoff")
}

func TestRescueDecisionPanickingBackoff(t *testing.T) {
	var buf bytes.Buffer
	c := newPoolClient(t, newFakeStore(), noopHandler[testPayload](), KindConfig{
		Backoff: func(int) time.Duration { panic("bad backoff") },
	}, WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))

	r := c.rescueDecision(store.StuckJob{ID: 1, Kind: 1, Attempt: 1, MaxAttempts: 3})
	require.False(t, r.d.Fail)
	require.Zero(t, r.d.Delay)
	require.Contains(t, buf.String(), "level=ERROR")
}
