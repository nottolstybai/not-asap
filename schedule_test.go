package notasap

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/internal/store"
)

type failStore struct {
	jobStore
	t *testing.T
}

func (f failStore) Insert(context.Context, store.DB, []store.InsertParams) ([]store.InsertResult, error) {
	f.t.Fatal("store must not be called")
	return nil, nil
}

type unknownPayload struct{}

func (unknownPayload) Kind() Kind { return 99 }

func TestScheduleOptionsValidation(t *testing.T) {
	c, err := newClient(failStore{t: t}, nil)
	require.NoError(t, err)
	require.NoError(t, Register(c, noopHandler[testPayload](), KindConfig{Name: "a"}))
	ctx := context.Background()

	t.Run("negative delay", func(t *testing.T) {
		_, err := c.Schedule(ctx, testPayload{}, WithDelay(-time.Second))
		require.ErrorContains(t, err, "delay must not be negative")
	})
	t.Run("empty key", func(t *testing.T) {
		_, err := c.Schedule(ctx, testPayload{}, WithIdempotencyKey(""))
		require.ErrorContains(t, err, "key is empty")
	})
	t.Run("replace without key", func(t *testing.T) {
		_, err := c.Schedule(ctx, testPayload{}, WithReplace())
		require.ErrorContains(t, err, "WithReplace requires WithIdempotencyKey")
	})
	t.Run("unknown kind", func(t *testing.T) {
		_, err := c.Schedule(ctx, unknownPayload{})
		require.ErrorIs(t, err, ErrUnknownKind)
		require.ErrorContains(t, err, "99")
	})
	t.Run("nil payload", func(t *testing.T) {
		_, err := c.Schedule(ctx, nil)
		require.ErrorContains(t, err, "payload is nil")
	})
	t.Run("batch rejected as a whole", func(t *testing.T) {
		_, err := c.ScheduleBatch(ctx, Item(testPayload{}), Item(unknownPayload{}))
		require.ErrorIs(t, err, ErrUnknownKind)
	})
	t.Run("nil tx", func(t *testing.T) {
		_, err := c.ScheduleTx(ctx, nil, testPayload{})
		require.ErrorContains(t, err, "tx is nil")
	})
	t.Run("empty batch", func(t *testing.T) {
		res, err := c.ScheduleBatch(ctx)
		require.NoError(t, err)
		require.Nil(t, res)
	})
}
