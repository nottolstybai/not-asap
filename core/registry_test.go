package core

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// testJobArgs is a minimal JobArgs implementation for testing.
type testJobArgs struct {
	Message string `json:"message"`
}

func (t testJobArgs) Kind() string { return "test-job" }

// countJobArgs is a JobArgs with an integer field for unmarshal testing.
type countJobArgs struct {
	N int `json:"n"`
}

func (c countJobArgs) Kind() string { return "count" }

func TestRegister_KindLinked(t *testing.T) {
	r := NewRegistry()
	var handled atomic.Bool
	Register(r, func(ctx context.Context, job testJobArgs) error {
		handled.Store(true)
		return nil
	})

	tc, handle, ok := r.Lookup("test-job")
	require.True(t, ok, "registered kind must be found")
	require.Equal(t, "test-job", testJobArgs{}.Kind())
	_ = tc // TaskConfig is zero-value since no opts passed
	_ = handle
	require.False(t, handled.Load(), "handler not called yet")
}

func TestRegister_PayloadUnmarshalled(t *testing.T) {
	r := NewRegistry()
	var received atomic.Int64
	Register(r, func(ctx context.Context, job countJobArgs) error {
		received.Store(int64(job.N))
		return nil
	})

	_, handle, ok := r.Lookup("count")
	require.True(t, ok)

	err := handle(context.Background(), []byte(`{"n":42}`))
	require.NoError(t, err)
	require.Equal(t, int64(42), received.Load(), "handler received unmarshalled N=42")
}

func TestRegister_UnmarshalError(t *testing.T) {
	r := NewRegistry()
	Register(r, func(ctx context.Context, job countJobArgs) error {
		return nil
	})

	_, handle, ok := r.Lookup("count")
	require.True(t, ok)

	err := handle(context.Background(), []byte("not-json"))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "unmarshal payload"), "error must mention unmarshal")
	require.True(t, strings.Contains(err.Error(), "count"), "error must include kind name")
}

func TestRegister_UnknownKind(t *testing.T) {
	r := NewRegistry()
	_, _, ok := r.Lookup("unregistered")
	require.False(t, ok, "unregistered kind must not be found")
}

func TestRegister_TaskConfigOption(t *testing.T) {
	r := NewRegistry()
	Register(r, func(ctx context.Context, job testJobArgs) error {
		return nil
	}, func(tc *TaskConfig) {
		tc.MaxRetries = 7
	})

	tc, _, ok := r.Lookup("test-job")
	require.True(t, ok)
	require.Equal(t, 7, tc.MaxRetries, "TaskConfig option must be applied")
}

func TestRegister_Overwrite(t *testing.T) {
	r := NewRegistry()
	var first, second atomic.Int64
	Register(r, func(ctx context.Context, job testJobArgs) error {
		first.Add(1)
		return nil
	})
	Register(r, func(ctx context.Context, job testJobArgs) error {
		second.Add(1)
		return nil
	})

	_, handle, ok := r.Lookup("test-job")
	require.True(t, ok)

	err := handle(context.Background(), []byte(`{"message":"hi"}`))
	require.NoError(t, err)
	require.Equal(t, int64(0), first.Load(), "first handler must be overwritten")
	require.Equal(t, int64(1), second.Load(), "second handler must be called")
}
