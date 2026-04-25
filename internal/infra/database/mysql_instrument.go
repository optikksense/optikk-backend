package database

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/jmoiron/sqlx"
)

// Parallel of the ClickHouse seam, wrapping sqlx on the MariaDB side.
// Repos call these instead of r.db.GetContext / SelectContext /
// ExecContext so we get the same (Prom histogram + error log)
// envelope. `op` convention stays `<module>.<MethodName>`.

// GetSQL scans a single row into dest. sqlx ErrNoRows is not wrapped
// here — the caller decides whether to treat "missing" as an error.
func GetSQL(ctx context.Context, db *sqlx.DB, op string, dest any, query string, args ...any) error {
	done := startSQLOp(ctx)
	start := time.Now()
	err := db.GetContext(ctx, dest, query, args...)
	done(err, start, op)
	return err
}

// SelectSQL scans a multi-row result into dest.
func SelectSQL(ctx context.Context, db *sqlx.DB, op string, dest any, query string, args ...any) error {
	done := startSQLOp(ctx)
	start := time.Now()
	err := db.SelectContext(ctx, dest, query, args...)
	done(err, start, op)
	return err
}

// ExecSQL runs an INSERT/UPDATE/DELETE/DDL statement.
func ExecSQL(ctx context.Context, db *sqlx.DB, op, query string, args ...any) (sql.Result, error) {
	done := startSQLOp(ctx)
	start := time.Now()
	res, err := db.ExecContext(ctx, query, args...)
	done(err, start, op)
	return res, err
}

func startSQLOp(ctx context.Context) func(error, time.Time, string) {
	return func(err error, start time.Time, op string) {
		dur := time.Since(start).Seconds()
		metrics.DBQueryDuration.WithLabelValues("mysql", op).Observe(dur)
		metrics.DBQueriesTotal.WithLabelValues("mysql", op, resultLabel(err)).Inc()
		if err != nil {
			slog.ErrorContext(ctx, "mysql query failed",
				slog.String("op", op),
				slog.Float64("duration_s", dur),
				slog.Any("error", err),
			)
		}
	}
}
