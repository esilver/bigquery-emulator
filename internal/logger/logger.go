package logger

import (
	"context"

	"go.uber.org/zap"
)

type loggerKey struct{}

func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func Logger(ctx context.Context) *zap.Logger {
	// Not every caller reaches here through the server's request path: the
	// YAML data loader (server.Load -> addProjects -> AddTableData) builds
	// its own context without a logger. Return a no-op logger instead of
	// panicking on the type assertion.
	if logger, ok := ctx.Value(loggerKey{}).(*zap.Logger); ok {
		return logger
	}
	return zap.NewNop()
}
