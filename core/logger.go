package core

import (
	"context"
	"log/slog"
)

// SlogValueKey is the context key used to store the enriched logger.
type SlogValueKey struct{}

// LoggerFromContext extracts the enriched logger from a job's context.
// Returns slog.Default() if no logger is found.
//
// Usage in a handler:
//
//	func MyHandler(ctx context.Context, args MyArgs) error {
//	    logger := core.LoggerFromContext(ctx)
//	    logger.Info("processing job")
//	    return nil
//	}
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(SlogValueKey{}).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}
