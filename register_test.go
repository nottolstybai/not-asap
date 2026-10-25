package notasap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/internal/store"
)

type testPayload struct {
	N int `json:"n"`
}

func (testPayload) Kind() Kind { return 1 }

type otherPayload struct{}

func (otherPayload) Kind() Kind { return 2 }

type zeroKindPayload struct{}

func (zeroKindPayload) Kind() Kind { return 0 }

type ptrPayload struct{}

func (*ptrPayload) Kind() Kind { return 3 }

func noop[T Payload](context.Context, *Job[T]) error { return nil }

func newTestClient(t *testing.T, opts ...Option) *Client {
	t.Helper()
	c, err := newClient(nil, nil, opts...)
	require.NoError(t, err)
	return c
}

func TestRegisterValidation(t *testing.T) {
	t.Parallel()
	t.Run("nil handler", func(t *testing.T) {
		t.Parallel()
		err := Register[testPayload](newTestClient(t), nil, KindConfig{Name: "a"})
		require.ErrorContains(t, err, "handler is nil")
	})
	t.Run("pointer type", func(t *testing.T) {
		t.Parallel()
		err := Register(newTestClient(t), noop[*ptrPayload], KindConfig{Name: "a"})
		require.ErrorContains(t, err, "non-pointer")
	})
	t.Run("kind not positive", func(t *testing.T) {
		t.Parallel()
		err := Register(newTestClient(t), noop[zeroKindPayload], KindConfig{Name: "a"})
		require.ErrorContains(t, err, "must be positive")
	})
	t.Run("duplicate kind", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t)
		require.NoError(t, Register(c, noop[testPayload], KindConfig{Name: "first"}))
		err := Register(c, noop[testPayload], KindConfig{Name: "second"})
		require.ErrorContains(t, err, "already registered")
		require.ErrorContains(t, err, "first")
	})
	t.Run("duplicate name", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t)
		require.NoError(t, Register(c, noop[testPayload], KindConfig{Name: "same"}))
		err := Register(c, noop[otherPayload], KindConfig{Name: "same"})
		require.ErrorContains(t, err, "already used")
		_, ok := c.entry(2)
		require.False(t, ok)
	})
	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()
		err := Register(newTestClient(t), noop[testPayload], KindConfig{})
		require.ErrorContains(t, err, "Name is empty")
	})
	t.Run("started", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t)
		c.state = stateStarted
		err := Register(c, noop[testPayload], KindConfig{Name: "a"})
		require.ErrorIs(t, err, ErrAlreadyStarted)
	})
	t.Run("stopped", func(t *testing.T) {
		t.Parallel()
		c := newTestClient(t)
		c.state = stateStopped
		err := Register(c, noop[testPayload], KindConfig{Name: "a"})
		require.ErrorIs(t, err, ErrStopped)
	})
}

func TestRegisterRunDecodesPayload(t *testing.T) {
	t.Parallel()
	c := newTestClient(t)
	want := errors.New("boom")
	var got *Job[testPayload]
	require.NoError(t, Register(c, func(_ context.Context, j *Job[testPayload]) error {
		got = j
		return want
	}, KindConfig{Name: "a"}))

	e, ok := c.entry(1)
	require.True(t, ok)

	decodeErr, handlerErr := e.run(context.Background(), store.ClaimedJob{
		ID: 7, Kind: 1, Payload: []byte(`{"n":42}`), Attempt: 2, MaxAttempts: 3,
	})
	require.NoError(t, decodeErr)
	require.ErrorIs(t, handlerErr, want)
	require.NotNil(t, got)
	require.EqualValues(t, 7, got.ID)
	require.Equal(t, Kind(1), got.Kind)
	require.Equal(t, 42, got.Payload.N)
	require.Equal(t, 2, got.Attempt)
	require.Equal(t, 3, got.MaxAttempts)
}

func TestRegisterRunDecodeError(t *testing.T) {
	t.Parallel()
	c := newTestClient(t)
	called := false
	require.NoError(t, Register(c, func(context.Context, *Job[testPayload]) error {
		called = true
		return nil
	}, KindConfig{Name: "a"}))

	e, _ := c.entry(1)
	decodeErr, handlerErr := e.run(context.Background(), store.ClaimedJob{ID: 1, Kind: 1, Payload: []byte("not-json")})
	require.Error(t, decodeErr)
	require.NoError(t, handlerErr)
	require.False(t, called)
}
