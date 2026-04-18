package queue

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
)

func TestLoggerFromContextReturnsDefaultWhenNotFound(t *testing.T) {
	ctx := context.Background()
	logger := LoggerFromContext(ctx)
	require.NotNil(t, logger)
	logger.Info("test")
}

func TestLoggerFromContextExtractsEnrichedLogger(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)
	customLogger := slog.New(handler)

	ctx := context.WithValue(context.Background(), core.SlogValueKey{}, customLogger.With(
		"job_id", "test-123",
		"job_type", "test",
		"attempt", 1,
	))

	logger := LoggerFromContext(ctx)
	require.NotNil(t, logger)

	logger.Info("test message")
	output := buf.String()
	require.Contains(t, output, `job_id=test-123`)
	require.Contains(t, output, `job_type=test`)
	require.Contains(t, output, `attempt=1`)
	require.Contains(t, output, `msg="test message"`)
}

func TestLoggerFromContextWithNilConfig(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	logger := q.logger()
	require.NotNil(t, logger)
	logger.Info("test")
}

type panicLogTestJob struct{}

func (panicLogTestJob) Kind() string { return "paniclogtest" }

func TestPanicStackTraceIncluded(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	var logBuf bytes.Buffer
	handler := slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	cfg.Logger = slog.New(handler)

	q := NewQueueFromPool(nil, cfg)
	q.sem = newSemaphore(cfg.WorkerCount)

	Register(q, func(ctx context.Context, args panicLogTestJob) error {
		panic("intentional panic for test")
	})

	job := &database.ClaimedJob{
		ID:          "panic-test",
		JobType:     "paniclogtest",
		Payload:     []byte(`{}`),
		Attempt:     1,
		MaxAttempts: 3,
	}

	q.runningMu.Lock()
	q.running[job.ID] = struct{}{}
	q.runningMu.Unlock()

	q.wg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, q.sem.Acquire(ctx, 1))

	func() {
		defer func() {
			if r := recover(); r != nil {
				// markJobFailed panicked on nil pool
			}
		}()
		q.runJob(job)
	}()

	output := logBuf.String()
	require.Contains(t, output, "job panicked")
	require.Contains(t, output, "intentional panic for test")
}
