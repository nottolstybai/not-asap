package notasap

import (
	"context"
	"log/slog"
)

type loggerKey struct{}

// Logger returns the logger attached to a handler's context, enriched with
// job fields, or slog.Default() if ctx carries none.
func Logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

func withLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}
