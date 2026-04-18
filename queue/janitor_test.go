package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/core"
)

func TestJanitorDisabledNoGoroutine(t *testing.T) {
	cfg, err := core.NewQueueConfig(func(c *core.QueueConfig) {
		c.Janitor.Disabled = true
		c.WorkerCount = 0
	})
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.Start()

	require.Nil(t, q.janitorDone)
	q.Stop(context.Background())
}

func TestJanitorStartsWhenEnabled(t *testing.T) {
	cfg, err := core.NewQueueConfig(func(c *core.QueueConfig) {
		c.Janitor.Disabled = false
		c.Janitor.RunInterval = time.Hour
		c.Janitor.TTL = time.Second
		c.Janitor.Statuses = []core.Status{core.StatusDone}
		c.WorkerCount = 0
	})
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.Start()

	require.NotNil(t, q.janitorDone)
	q.Stop(context.Background())
}

func TestJanitorStopClosesChannel(t *testing.T) {
	cfg, err := core.NewQueueConfig(func(c *core.QueueConfig) {
		c.Janitor.RunInterval = time.Hour
		c.Janitor.TTL = time.Second
		c.Janitor.Statuses = []core.Status{core.StatusDone}
		c.WorkerCount = 0
	})
	require.NoError(t, err)

	q := NewQueueFromPool(nil, cfg)
	q.Start()

	time.Sleep(10 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, q.Stop(ctx))
}
