// Package slogx holds slog handler middleware used by the app's root
// logger. FanoutHandler tees records to multiple inner handlers (for
// example a stdout handler alongside a file handler) so we can write to
// more than one sink without building ad-hoc logging code at each call
// site.
package slogx

import (
	"context"
	"log/slog"
)

// FanoutHandler forwards each record to every inner handler in order.
// `Enabled` returns true if ANY inner handler would accept the record.
type FanoutHandler struct{ handlers []slog.Handler }

func NewFanoutHandler(handlers ...slog.Handler) *FanoutHandler {
	return &FanoutHandler{handlers: handlers}
}

func (h *FanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, inner := range h.handlers {
		if inner.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *FanoutHandler) Handle(ctx context.Context, rec slog.Record) error {
	var firstErr error
	for _, inner := range h.handlers {
		if !inner.Enabled(ctx, rec.Level) {
			continue
		}
		if err := inner.Handle(ctx, rec.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (h *FanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, inner := range h.handlers {
		next[i] = inner.WithAttrs(attrs)
	}
	return &FanoutHandler{handlers: next}
}

func (h *FanoutHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, inner := range h.handlers {
		next[i] = inner.WithGroup(name)
	}
	return &FanoutHandler{handlers: next}
}
