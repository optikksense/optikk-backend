package database

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const (
	chMaxOpenConns    = 200
	chMaxIdleConns    = 100
	chConnMaxLifetime = 30 * time.Minute
	chDialTimeout     = 5 * time.Second
)

func OpenClickHouseConn(dsn string) (clickhouse.Conn, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse DSN: %w", err)
	}

	opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	opts.MaxOpenConns = chMaxOpenConns
	opts.MaxIdleConns = chMaxIdleConns
	opts.ConnMaxLifetime = chConnMaxLifetime
	opts.DialTimeout = chDialTimeout
	opts.ConnOpenStrategy = clickhouse.ConnOpenRoundRobin

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

// Per-budget ClickHouse settings, applied via clickhouse.Context. Three
// budgets, three flat literals — no struct, no helper, no conditional. Each
// budget owns its own overflow modes; Explorer uses "throw" because the
// CH query cache requires it.

var dashboardSettings = clickhouse.Settings{
	"max_execution_time":     3,
	"max_rows_to_read":       20_000_000,
	"max_memory_usage":       1 * 1024 * 1024 * 1024,
	"max_result_rows":        10_000,
	"result_overflow_mode":   "break",
	"read_overflow_mode":     "break",
	"optimize_read_in_order": 1,
}

var overviewSettings = clickhouse.Settings{
	"max_execution_time":     15,
	"max_rows_to_read":       100_000_000,
	"max_memory_usage":       2 * 1024 * 1024 * 1024,
	"max_result_rows":        100_000,
	"result_overflow_mode":   "break",
	"read_overflow_mode":     "break",
	"optimize_read_in_order": 1,
}

var explorerSettings = clickhouse.Settings{
	"max_execution_time":              60,
	"max_rows_to_read":                1_000_000_000,
	"max_memory_usage":                8 * 1024 * 1024 * 1024,
	"max_result_rows":                 1_000_000,
	"result_overflow_mode":            "throw",
	"read_overflow_mode":              "throw",
	"optimize_read_in_order":          1,
	"use_query_cache":                 1,
	"query_cache_ttl":                 60,
	"query_cache_share_between_users": 0,
}

func DashboardCtx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(dashboardSettings))
}

func OverviewCtx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(overviewSettings))
}

func ExplorerCtx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(explorerSettings))
}

func SpanBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func SimpleBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}
