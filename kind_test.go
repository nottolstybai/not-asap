package notasap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusString(t *testing.T) {
	cases := map[Status]string{
		StatusPending:   "pending",
		StatusRunning:   "running",
		StatusDone:      "done",
		StatusFailed:    "failed",
		StatusCancelled: "cancelled",
	}
	for s, want := range cases {
		require.Equal(t, want, s.String())
	}
	require.Equal(t, "status(9)", Status(9).String())
}
