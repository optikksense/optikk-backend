package httputil

import (
	"context"
	"errors"
	"log/slog"
)

// LogQueryErr logs DB/query errors at the right level: debug for client abort,
// warn for deadlines (slow/overloaded), error for unexpected failures.
func LogQueryErr(msg string, err error, attrs ...slog.Attr) {
	if err == nil {
		return
	}
	parts := make([]any, 0, 2+len(attrs))
	parts = append(parts, slog.Any("error", err))
	for _, a := range attrs {
		parts = append(parts, a)
	}
	switch {
	case errors.Is(err, context.Canceled):
		slog.Debug(msg, parts...)
	case errors.Is(err, context.DeadlineExceeded):
		slog.Warn(msg, parts...)
	default:
		slog.Error(msg, parts...)
	}
}
