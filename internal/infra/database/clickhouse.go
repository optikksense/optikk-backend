package database

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// Connection pool sizing. Values are set unconditionally on the Options
// returned from ParseDSN — any DSN-embedded pool hints are overwritten.
const (
	chMaxOpenConns    = 100
	chMaxIdleConns    = 25
	chConnMaxLifetime = 30 * time.Minute
)

// OpenClickHouseConn parses dsn, opens a connection pool, and pings. The DSN
// carries everything: host, credentials, database, and TLS via ?secure=true.
// There is no prod-vs-dev branch — prod config emits a secure DSN.
func OpenClickHouseConn(dsn string) (clickhouse.Conn, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse DSN: %w", err)
	}

	opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	opts.MaxOpenConns = chMaxOpenConns
	opts.MaxIdleConns = chMaxIdleConns
	opts.ConnMaxLifetime = chConnMaxLifetime

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

// QueryBudget is the typed resource ceiling applied per-query via
// clickhouse.WithSettings. Fields map directly to ClickHouse server settings.
type QueryBudget struct {
	MaxExecutionTime    int // seconds
	MaxRowsToRead       int64
	MaxMemoryUsage      int64 // bytes
	MaxResultRows       int64
	ResultOverflowMode  string
	ReadOverflowMode    string
	OptimizeReadInOrder int
	// UseQueryCache turns on the CH server-side query cache for this budget.
	// Safe for tenant-scoped reads where the same (team_id, query) is hit often.
	UseQueryCache bool
}

func (b QueryBudget) settings() clickhouse.Settings {
	s := clickhouse.Settings{
		"max_execution_time":     b.MaxExecutionTime,
		"max_rows_to_read":       b.MaxRowsToRead,
		"max_memory_usage":       b.MaxMemoryUsage,
		"max_result_rows":        b.MaxResultRows,
		"result_overflow_mode":   b.ResultOverflowMode,
		"read_overflow_mode":     b.ReadOverflowMode,
		"optimize_read_in_order": b.OptimizeReadInOrder,
	}
	if b.UseQueryCache {
		s["use_query_cache"] = 1
		s["query_cache_ttl"] = 60
		// Keep results per-team; trace reads are tenant-scoped.
		s["query_cache_share_between_users"] = 0
	}
	return s
}

// Ctx returns a ctx with this budget attached. Repositories call OverviewCtx
// or ExplorerCtx below rather than using this directly.
func (b QueryBudget) Ctx(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(b.settings()))
}

// Overview — cheap dashboard / overview / infrastructure / saturation /
// HTTP-metrics / services budget. Fail fast if a dashboard query runs long.
var Overview = QueryBudget{
	MaxExecutionTime:    15,
	MaxRowsToRead:       100_000_000,
	MaxMemoryUsage:      2 * 1024 * 1024 * 1024,
	MaxResultRows:       100_000,
	ResultOverflowMode:  "break",
	ReadOverflowMode:    "break",
	OptimizeReadInOrder: 1,
}

// Explorer — ad-hoc explorer / tracedetail / ai-explorer / alerting-backtest
// budget. Legitimately scans 10× more data than Overview.
var Explorer = QueryBudget{
	MaxExecutionTime:    60,
	MaxRowsToRead:       1_000_000_000,
	MaxMemoryUsage:      8 * 1024 * 1024 * 1024,
	MaxResultRows:       1_000_000,
	ResultOverflowMode:  "break",
	ReadOverflowMode:    "break",
	OptimizeReadInOrder: 1,
	// Trace reads are tenant-scoped and repeat-heavy (users re-opening the same
	// trace, rapid list-page interactions). CH's query cache makes hot-path
	// hits free. TTL 60s is short enough that new ingest shows up quickly.
	UseQueryCache: true,
}

// OverviewCtx returns ctx with the overview query budget attached.
func OverviewCtx(ctx context.Context) context.Context { return Overview.Ctx(ctx) }

// ExplorerCtx returns ctx with the explorer query budget attached.
func ExplorerCtx(ctx context.Context) context.Context { return Explorer.Ctx(ctx) }

// SelectTyped executes a SELECT and scans into a typed slice. Callers must
// pass a ctx already wrapped with OverviewCtx or ExplorerCtx.
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

// SpanBaseParams returns the standard named parameters for span queries:
// team_id, ts_bucket_start range, and timestamp range.
func SpanBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// SimpleBaseParams returns named parameters for queries that don't need
// bucket range pruning (metrics, infrastructure, saturation, etc.).
func SimpleBaseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 - domain-constrained value
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}
