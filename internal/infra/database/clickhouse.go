package database

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const (
	defaultCHMaxOpenConns    = 15
	defaultCHMaxIdleConns    = 5
	defaultCHConnMaxLifetime = 30 * time.Minute
)

func applyClickHouseConnectionPoolDefaults(opts *clickhouse.Options) {
	if opts.MaxOpenConns <= 0 {
		opts.MaxOpenConns = defaultCHMaxOpenConns
	}
	if opts.MaxIdleConns <= 0 {
		opts.MaxIdleConns = defaultCHMaxIdleConns
	}
	if opts.ConnMaxLifetime <= 0 {
		opts.ConnMaxLifetime = defaultCHConnMaxLifetime
	}
}

type ClickHouseCloudConfig struct {
	Host     string
	Username string
	Password string
}

func OpenClickHouseConn(dsn string, isProduction bool, cloud ...ClickHouseCloudConfig) (clickhouse.Conn, error) {
	var opts *clickhouse.Options

	if isProduction {
		if len(cloud) == 0 {
			return nil, errors.New("clickhouse: ClickHouseCloudConfig required for production mode")
		}
		cc := cloud[0]
		opts = &clickhouse.Options{
			Addr:     []string{cc.Host},
			Protocol: clickhouse.Native,
			TLS: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
			Auth: clickhouse.Auth{
				Username: cc.Username,
				Password: cc.Password,
			},
			DialTimeout: 5 * time.Second,
			ReadTimeout: 30 * time.Second,
		}
	} else {
		var err error
		opts, err = clickhouse.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("clickhouse: parse DSN: %w", err)
		}
	}

	if opts.Compression == nil {
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}

	applyClickHouseConnectionPoolDefaults(opts)

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		return nil, err
	}

	return conn, nil
}

// Query budgets applied per-query via clickhouse.WithSettings. Use OverviewCtx
// for cheap dashboard / overview / infrastructure / saturation / HTTP-metrics
// / services queries, and ExplorerCtx for ad-hoc explorer / tracedetail /
// ai-explorer / alerting-backtest queries that legitimately scan more data.
var (
	overviewSettings = clickhouse.Settings{
		"max_execution_time":     15,
		"max_rows_to_read":       100_000_000,
		"max_memory_usage":       2_147_483_648,
		"max_result_rows":        100_000,
		"result_overflow_mode":   "break",
		"read_overflow_mode":     "break",
		"optimize_read_in_order": 1,
	}
	explorerSettings = clickhouse.Settings{
		"max_execution_time":     60,
		"max_rows_to_read":       1_000_000_000,
		"max_memory_usage":       8_589_934_592,
		"max_result_rows":        1_000_000,
		"result_overflow_mode":   "break",
		"read_overflow_mode":     "break",
		"optimize_read_in_order": 1,
	}
)

func copySettings(src clickhouse.Settings) clickhouse.Settings {
	dst := make(clickhouse.Settings, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// OverviewCtx returns ctx with the overview query budget attached —
// 15s / 100M rows / 2 GB. The settings ride on the context via
// clickhouse.WithSettings and are read by clickhouse-go at query time.
func OverviewCtx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(copySettings(overviewSettings)))
}

// ExplorerCtx returns ctx with the explorer query budget attached —
// 60s / 1B rows / 8 GB.
func ExplorerCtx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(copySettings(explorerSettings)))
}

// OverviewSettings returns a defensive copy of the overview budget. Tests
// assert on this map; production code should use OverviewCtx.
func OverviewSettings() clickhouse.Settings { return copySettings(overviewSettings) }

// ExplorerSettings returns a defensive copy of the explorer budget.
func ExplorerSettings() clickhouse.Settings { return copySettings(explorerSettings) }

// SelectTyped executes a SELECT and scans into a typed slice. Callers must
// pass a ctx already wrapped with OverviewCtx or ExplorerCtx to apply a budget.
func SelectTyped[T any](ctx context.Context, db clickhouse.Conn, query string, args ...any) ([]T, error) {
	var rows []T
	err := db.Select(ctx, &rows, query, args...)
	return rows, err
}

// QueryRowTyped executes a single-row SELECT and scans into a typed struct.
// Callers must pass a ctx already wrapped with OverviewCtx or ExplorerCtx.
func QueryRowTyped[T any](ctx context.Context, db clickhouse.Conn, query string, args ...any) (T, error) {
	var row T
	err := db.QueryRow(ctx, query, args...).ScanStruct(&row)
	return row, err
}

// SpanBaseParams returns the standard named parameters for span queries.
// Includes team_id, ts_bucket_start range, and timestamp range.
func SpanBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// SimpleBaseParams returns the standard named parameters for queries that
// do not need bucket range pruning (metrics, infrastructure, saturation, etc.).
func SimpleBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}
