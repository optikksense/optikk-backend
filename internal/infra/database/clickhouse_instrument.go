package database

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Three-function façade around every ClickHouse read/write. Every
// repository in internal/modules/ calls these instead of r.db.Select /
// r.db.Query / r.db.Exec so we get a uniform (span + Prom histogram +
// structured error log) envelope for free.
//
// The caller is still responsible for wrapping ctx with the correct
// QueryBudget — i.e. dbutil.SelectCH(dbutil.ExplorerCtx(ctx), ...).
// We do not swallow errors.
//
// `op` must be stable and unique per query site — the convention is
// `<module>.<MethodName>` (e.g. `"logs.ListLogs"`). Duplicate op
// labels double-bucket the histogram.

var chTracer = otel.Tracer("optikk-backend/clickhouse")

// SelectCH runs a multi-row read into dest. dest must be a pointer to
// a slice of DTOs, per the clickhouse-go/v2 Conn.Select contract.
func SelectCH(ctx context.Context, conn clickhouse.Conn, op string, dest any, query string, args ...any) error {
	ctx, done := startCHSpan(ctx, op, query)
	start := time.Now()
	err := conn.Select(ctx, dest, query, args...)
	done(err, start, op)
	return err
}

// QueryCH runs a ClickHouse read and hands back the raw driver.Rows so
// the caller can scan row-by-row. Used where SelectCH's slice mode does
// not fit (e.g. streaming cursors, heterogeneous columns).
func QueryCH(ctx context.Context, conn clickhouse.Conn, op, query string, args ...any) (driver.Rows, error) {
	ctx, done := startCHSpan(ctx, op, query)
	start := time.Now()
	rows, err := conn.Query(ctx, query, args...)
	done(err, start, op)
	return rows, err
}

// ExecCH runs a DDL / write statement. No rows returned.
func ExecCH(ctx context.Context, conn clickhouse.Conn, op, query string, args ...any) error {
	ctx, done := startCHSpan(ctx, op, query)
	start := time.Now()
	err := conn.Exec(ctx, query, args...)
	done(err, start, op)
	return err
}

func startCHSpan(ctx context.Context, op, stmt string) (context.Context, func(error, time.Time, string)) {
	ctx, span := chTracer.Start(ctx, "db.clickhouse."+op, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(
		attribute.String("db.system", "clickhouse"),
		attribute.String("db.operation", op),
		attribute.String("db.statement", truncateStmt(stmt)),
	)
	return ctx, func(err error, start time.Time, op string) {
		dur := time.Since(start).Seconds()
		metrics.DBQueryDuration.WithLabelValues("clickhouse", op).Observe(dur)
		metrics.DBQueriesTotal.WithLabelValues("clickhouse", op, resultLabel(err)).Inc()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			slog.ErrorContext(ctx, "clickhouse query failed",
				slog.String("op", op),
				slog.Float64("duration_s", dur),
				slog.Any("error", err),
			)
		}
		span.End()
	}
}
