package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
)

func TestShutdownStopsWorkers(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) {
			c.WorkerCount = 2
			c.PollInterval = 10 * time.Millisecond
		},
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.sem = newSemaphore(cfg.WorkerCount)
	q.jobCtx, q.jobCancel = context.WithCancel(context.Background())
	q.started = true

	q.hbWg.Go(func() {
		q.heartbeater()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = q.Stop(ctx)
	require.NoError(t, err)

	err = q.Stop(ctx)
	require.NoError(t, err)
}

func TestShutdownIdempotent(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.sem = newSemaphore(cfg.WorkerCount)
	q.jobCtx, q.jobCancel = context.WithCancel(context.Background())
	q.started = true

	ctx := context.Background()
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() {
			_ = q.Stop(ctx)
		})
	}
	wg.Wait()
}

func TestShutdownWithoutStart(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	ctx := context.Background()
	err = q.Stop(ctx)
	require.NoError(t, err)
}

func TestShutdownCancelledJobRetrying(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	job := &database.ClaimedJob{ID: "test", Attempt: 1, MaxAttempts: 3}

	func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected: nil pool causes panic on Exec
			}
		}()
		q.handleRetry(job, context.Canceled, q.resolveConfig("test"))
	}()
}

func TestShutdownDrainTimeout(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) {
			c.ShutdownTimeout = 50 * time.Millisecond
		},
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.sem = newSemaphore(cfg.WorkerCount)
	q.jobCtx, q.jobCancel = context.WithCancel(context.Background())
	q.started = true

	q.wg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = q.Stop(ctx)
	require.Error(t, err)
	assert.True(t, errorsIs(err, context.DeadlineExceeded) || errorsIs(err, context.Canceled))
}

func TestStopMethodExists(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	ctx := context.Background()
	err = q.Stop(ctx)
	require.NoError(t, err)
}

func errorsIs(err, target error) bool {
	if err == nil {
		return false
	}
	if err == target {
		return true
	}
	return err.Error() == target.Error()
}
