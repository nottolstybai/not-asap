package notasap

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoggerFromContext(t *testing.T) {
	t.Parallel()
	require.Same(t, slog.Default(), Logger(context.Background()))

	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	Logger(withLogger(context.Background(), l)).Info("hello")
	require.Contains(t, buf.String(), "hello")
}
