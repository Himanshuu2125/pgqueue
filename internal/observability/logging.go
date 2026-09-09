package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// SetupLogging installs a JSON slog default logger tagged with the service name
// and, for workers, the worker id.
func SetupLogging(service, workerID, level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		lvl = slog.LevelInfo
	}
	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})).With("service", service)
	if workerID != "" {
		l = l.With("worker_id", workerID)
	}
	slog.SetDefault(l)
	return l
}

// TraceID returns the trace id of the span in ctx, or "".
func TraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}
