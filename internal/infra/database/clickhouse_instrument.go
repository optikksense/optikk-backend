package database

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
)

// Three-function façade around every ClickHouse read/write. Every
// repository in internal/modules/ calls these instead of r.db.Select /
// r.db.Query / r.db.Exec so we get a uniform (Prom histogram +
// structured error log) envelope for free.
//
// The caller is still responsible for wrapping ctx with the correct
// QueryBudget — i.e. dbutil.SelectCH(dbutil.ExplorerCtx(ctx), ...).
// We do not swallow errors.
//
// `op` must be stable and unique per query site — the convention is
// `<module>.<MethodName>` (e.g. `"logs.ListLogs"`). Duplicate op
// labels double-bucket the histogram.

// SelectCH runs a multi-row read into dest. dest must be a pointer to
// a slice of DTOs, per the clickhouse-go/v2 Conn.Select contract.
func SelectCH(ctx context.Context, conn clickhouse.Conn, op string, dest any, query string, args ...any) error {
	done := startCHOp(ctx)
	start := time.Now()
	err := conn.Select(ctx, dest, query, args...)
	done(err, start, op)
	return err
}

// QueryCH runs a ClickHouse read and hands back the raw driver.Rows so
// the caller can scan row-by-row. Used where SelectCH's slice mode does
// not fit (e.g. streaming cursors, heterogeneous columns).
func QueryCH(ctx context.Context, conn clickhouse.Conn, op, query string, args ...any) (driver.Rows, error) {
	done := startCHOp(ctx)
	start := time.Now()
	rows, err := conn.Query(ctx, query, args...)
	done(err, start, op)
	return rows, err
}

// ExecCH runs a DDL / write statement. No rows returned.
func ExecCH(ctx context.Context, conn clickhouse.Conn, op, query string, args ...any) error {
	done := startCHOp(ctx)
	start := time.Now()
	err := conn.Exec(ctx, query, args...)
	done(err, start, op)
	return err
}

func startCHOp(ctx context.Context) func(error, time.Time, string) {
	return func(err error, start time.Time, op string) {
		dur := time.Since(start).Seconds()
		metrics.DBQueryDuration.WithLabelValues("clickhouse", op).Observe(dur)
		metrics.DBQueriesTotal.WithLabelValues("clickhouse", op, resultLabel(err)).Inc()
		if err != nil {
			slog.ErrorContext(ctx, "clickhouse query failed",
				slog.String("op", op),
				slog.Float64("duration_s", dur),
				slog.Any("error", err),
			)
		}
	}
}
