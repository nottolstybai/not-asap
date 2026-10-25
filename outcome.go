package notasap

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"
	"unicode/utf8"
)

// maxErrorLen is the maximum stored error text length in bytes.
const maxErrorLen = 4096

var (
	// errShutdown is the context cause when the client is stopping.
	errShutdown = errors.New("notasap: shutdown")
	// errTimeout is the context cause when a job exceeds its timeout.
	errTimeout = errors.New("notasap: job timeout")
)

// execResult is what running a handler produced.
type execResult struct {
	err        error
	panicked   bool
	panicValue any
	decodeErr  error
	cause      error
}

// action is the state transition chosen for an execResult; StatusPending
// means the job is rescheduled.
type action struct {
	status Status
	delay  time.Duration
	refund bool
	errMsg string
	reason RetryReason
}

// decide maps an execution result to the next job state. It is pure apart
// from logging a panicking backoff to logger.
func decide(r execResult, attempt, maxAttempts int, backoff Backoff, logger *slog.Logger) action {
	switch {
	case r.decodeErr != nil:
		return action{
			status: StatusFailed,
			errMsg: truncateError("decode payload: " + r.decodeErr.Error()),
		}
	case !r.panicked && r.err == nil:
		return action{status: StatusDone}
	case !r.panicked && errors.Is(r.err, ErrCancel):
		return action{
			status: StatusCancelled,
			errMsg: truncateError(r.err.Error()),
		}
	}

	errMsg := truncateError(failureText(r))
	switch {
	case errors.Is(r.cause, errShutdown):
		return action{
			status: StatusPending,
			refund: true,
			errMsg: errMsg,
			reason: RetryShutdown,
		}
	case !r.panicked && errors.Is(r.err, ErrKeepAttempt):
		return action{
			status: StatusPending,
			delay:  delayOf(backoff, attempt, logger),
			refund: true,
			errMsg: errMsg,
			reason: RetryKeepAttempt,
		}
	}

	reason := RetryError
	switch {
	case r.panicked:
		reason = RetryPanic
	case errors.Is(r.cause, errTimeout):
		reason = RetryTimeout
	}

	if attempt >= maxAttempts {
		return action{
			status: StatusFailed,
			errMsg: errMsg,
			reason: reason,
		}
	}

	return action{
		status: StatusPending,
		delay:  delayOf(backoff, attempt, logger),
		errMsg: errMsg,
		reason: reason,
	}
}

func failureText(r execResult) string {
	if r.panicked {
		return fmt.Sprintf("panic: %v", r.panicValue)
	}

	return r.err.Error()
}

// delayOf returns the backoff delay for attempt, clamped at 0. A panicking
// backoff is logged and yields 0.
func delayOf(b Backoff, attempt int, logger *slog.Logger) (d time.Duration) {
	defer func() {
		if v := recover(); v != nil {
			logger.Error("notasap: backoff panicked", "attempt", attempt, "panic", v, "stack", string(debug.Stack()))
			d = 0
		}
	}()

	return max(b(attempt), 0)
}

// truncateError makes s storable as text, replacing invalid UTF-8 with U+FFFD
// and dropping NUL bytes, which Postgres rejects, and cuts it to at most
// maxErrorLen bytes on a rune boundary.
func truncateError(s string) string {
	s = strings.ReplaceAll(strings.ToValidUTF8(s, "\uFFFD"), "\x00", "")
	if len(s) <= maxErrorLen {
		return s
	}

	n := maxErrorLen
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}

	return s[:n]
}
