package database

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const maxStmtBytes = 4096

// Traced opens a span around a ClickHouse read so we get per-query timing
// in the monitoring stack's trace view. Returns the child context (so the
// CH driver's own context ends up linked to the parent span) and a closer
// that records the terminal error + ends the span.
//
//	ctx, done := database.Traced(ctx, "logs.ListLogs", query)
//	defer done(err)
//	err = r.db.Select(ctx, ...)
func Traced(ctx context.Context, op, stmt string) (context.Context, func(err error)) {
	tracer := otel.Tracer("optikk-backend/clickhouse")
	ctx, span := tracer.Start(ctx, "db.clickhouse."+op, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("db.system", "clickhouse"),
		attribute.String("db.operation", op),
		attribute.String("db.statement", truncate(stmt, maxStmtBytes)),
	)
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…[truncated]"
}
