package core_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/nottolstybai/not-asap/core"
)

func TestStatus_Constants(t *testing.T) {
	require.Equal(t, "new", string(core.StatusNew))
	require.Equal(t, "running", string(core.StatusRunning))
	require.Equal(t, "done", string(core.StatusDone))
	require.Equal(t, "failed", string(core.StatusFailed))
	require.Equal(t, "retrying", string(core.StatusRetrying))
	require.Equal(t, "exhausted", string(core.StatusExhausted))
	require.Equal(t, "cancelled", string(core.StatusCancelled))
}

func TestErrCancel(t *testing.T) {
	require.NotNil(t, core.ErrCancel)
	require.True(t, errors.Is(core.ErrCancel, core.ErrCancel))
	require.Contains(t, core.ErrCancel.Error(), "cancelled")
}
