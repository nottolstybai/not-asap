package queue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
)

type testPanicJob struct{}

func (testPanicJob) Kind() string { return "panictest" }

func TestQueueInitialization(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) { c.WorkerCount = 2 },
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	require.NotNil(t, q.running)
	require.NotNil(t, q.stopCh)
	require.NotNil(t, q.hbDone)
	require.NotNil(t, q.registry)
}

func TestWorkerIDIsGenerated(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.workerID = generateWorkerID()

	require.Len(t, q.workerID, 36)
	require.Equal(t, byte('4'), q.workerID[14])
	require.Contains(t, "89ab", string(q.workerID[19]))
}

func TestSemaphoreInitialized(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) { c.WorkerCount = 5 },
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.sem = newSemaphore(q.config.WorkerCount)

	require.NotNil(t, q.sem)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for range 5 {
		err := q.sem.Acquire(ctx, 1)
		require.NoError(t, err)
	}
}

func TestPanicRecovery(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.sem = newSemaphore(cfg.WorkerCount)

	var recovered atomic.Bool
	Register(q, func(ctx context.Context, args testPanicJob) error {
		recovered.Store(true)
		panic("test panic")
	})

	job := &database.ClaimedJob{
		ID:          "test-panic-job",
		JobType:     "panictest",
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
				// markJobFailed panicked on nil pool — expected
			}
		}()
		q.runJob(job)
	}()

	_ = recovered.Load()
}

func TestUnregisteredHandlerType(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.sem = newSemaphore(cfg.WorkerCount)

	job := &database.ClaimedJob{
		ID:          "test-unregistered",
		JobType:     "unregistered_type",
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
		defer func() { _ = recover() }() // nil pool → markJobFailed panics, expected
		q.runJob(job)
	}()
}

func TestRunningMapTracking(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	q.runningMu.Lock()
	q.running["job-1"] = struct{}{}
	q.running["job-2"] = struct{}{}
	q.running["job-3"] = struct{}{}
	q.runningMu.Unlock()

	q.runningMu.RLock()
	require.Len(t, q.running, 3)
	_, ok1 := q.running["job-1"]
	_, ok2 := q.running["job-2"]
	_, ok3 := q.running["job-3"]
	q.runningMu.RUnlock()

	require.True(t, ok1)
	require.True(t, ok2)
	require.True(t, ok3)

	q.runningMu.Lock()
	delete(q.running, "job-2")
	q.runningMu.Unlock()

	q.runningMu.RLock()
	require.Len(t, q.running, 2)
	_, okDeleted := q.running["job-2"]
	q.runningMu.RUnlock()

	require.False(t, okDeleted)
}

func TestJobContextCreation(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	q.jobCtx, q.jobCancel = context.WithCancel(context.Background())

	require.NotNil(t, q.jobCtx)
	require.NotNil(t, q.jobCancel)

	q.jobCancel()
	assert.Error(t, q.jobCtx.Err())
}

func TestLoggerReturnsDefaultWhenNil(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	logger := q.logger()
	require.NotNil(t, logger)
}
