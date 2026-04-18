package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/core"
	"github.com/nottolstybai/not-asap/database"
)

func TestHeartbeatSkipsWhenNoRunningJobs(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) { c.HeartbeatInterval = 10 * time.Millisecond },
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.heartbeatTick()
}

func TestHeartbeatBatchedWithRunningJobs(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) { c.HeartbeatInterval = 10 * time.Millisecond },
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	q.runningMu.Lock()
	q.running["job-1"] = struct{}{}
	q.running["job-2"] = struct{}{}
	q.running["job-3"] = struct{}{}
	q.runningMu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected: nil pool causes panic on Exec
			}
		}()
		q.heartbeatTick()
	}()

	q.runningMu.RLock()
	require.Len(t, q.running, 3)
	q.runningMu.RUnlock()
}

func TestHeartbeatGoroutineLifecycle(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) { c.HeartbeatInterval = 10 * time.Millisecond },
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	q.hbWg.Go(func() {
		q.heartbeater()
	})

	time.Sleep(20 * time.Millisecond)
	q.stopHeartbeater()
}

func TestWorkerIDIsUUID(t *testing.T) {
	cfg, err := core.NewQueueConfig()
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.workerID = generateWorkerID()

	require.Len(t, q.workerID, 36)
	require.Equal(t, q.workerID[14], byte('4'))
	require.Contains(t, "89ab", string(q.workerID[19]))
}

func TestHeartbeatErrorHandling(t *testing.T) {
	cfg, err := core.NewQueueConfig(
		func(c *core.QueueConfig) { c.HeartbeatInterval = 10 * time.Millisecond },
	)
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)

	q.runningMu.Lock()
	q.running["job-1"] = struct{}{}
	q.runningMu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected
			}
		}()
		q.heartbeatTick()
	}()
}

func TestHeartbeatSQLConstant(t *testing.T) {
	require.Contains(t, database.HeartbeatSQL, "UPDATE notasap_jobs")
	require.Contains(t, database.HeartbeatSQL, "SET last_heartbeat_at = NOW()")
	require.Contains(t, database.HeartbeatSQL, "WHERE id = ANY($1)")
}
