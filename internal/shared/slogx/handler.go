// Package slogx holds slog handler middleware used by the app's root
// logger. The two exports wired into cmd/server/logger.go are:
//
//   - TraceAttrHandler — pulls the active OTel trace+span IDs off ctx
//     and decorates each record with them, so every slog line correlates
//     back to the Tempo trace in Grafana.
//   - FanoutHandler — tees records to multiple inner handlers so we
//     write to stdout (local dev) and to the OTel logs bridge (Loki)
//     at the same time.
package slogx

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/trace"
)

// TraceAttrHandler wraps an inner slog.Handler. For every record with a
// valid OTel span context it adds trace_id + span_id attributes before
// delegating. Records emitted without an active context pass through
// unchanged.
type TraceAttrHandler struct{ inner slog.Handler }

func NewTraceAttrHandler(inner slog.Handler) *TraceAttrHandler {
	return &TraceAttrHandler{inner: inner}
}

func (h *TraceAttrHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *TraceAttrHandler) Handle(ctx context.Context, rec slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, rec)
}

func (h *TraceAttrHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceAttrHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *TraceAttrHandler) WithGroup(name string) slog.Handler {
	return &TraceAttrHandler{inner: h.inner.WithGroup(name)}
}

// FanoutHandler forwards each record to every inner handler in order.
// `Enabled` returns true if ANY inner handler would accept the record —
// we use the permissive OR so the OTel bridge can still ship debug
// records to Loki even when stdout filters at Info.
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

// NewOtelBridgeHandler returns the OTel slog bridge bound to the current
// global logger provider. Service name shows up on each log record as
// the `service.name` resource attribute via the provider's resource.
func NewOtelBridgeHandler(serviceName string) slog.Handler {
	return otelslog.NewHandler(serviceName,
		otelslog.WithLoggerProvider(global.GetLoggerProvider()),
	)
}
