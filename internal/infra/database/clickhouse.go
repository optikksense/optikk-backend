package database

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// clickHouseTransientErrorSubstrings matches transport-layer failures worth retrying
// and mirrors the circuit-breaker "unsuccessful" string checks.
var clickHouseTransientErrorSubstrings = []string{
	"connection refused",
	"connection reset",
	"broken pipe",
	"i/o timeout",
	"no such host",
	"timeout exceeded",
	"connection closed",
	"unexpected eof",
}

const (
	defaultCHMaxOpenConns    = 15
	defaultCHMaxIdleConns    = 5
	defaultCHConnMaxLifetime = 30 * time.Minute
)

// applyClickHouseConnectionPoolDefaults sets pool sizing and connection rotation when
// unset so idle connections are recycled before typical LB / server idle cuts.
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


var prewhereRe = regexp.MustCompile(
	`(?i)\bWHERE\s+([\w.]*\.?)team_id\s*=\s*\?\s+AND\s+([\w.]*\.?)ts_bucket_start\s+BETWEEN\s+\?\s+AND\s+\?\s+AND\s+`,
)

func injectPrewhere(query string) string {
	if strings.Contains(strings.ToUpper(query), "PREWHERE") {
		return query
	}
	return prewhereRe.ReplaceAllString(query, "PREWHERE ${1}team_id = ? AND ${2}ts_bucket_start BETWEEN ? AND ? WHERE ")
}

// QueryProfile encapsulates the CH resource budget applied to a query.
// Phase 4 (2026-04-17 audit) splits the single hardcoded budget into
// workload-appropriate profiles: cheap overview / dashboard queries should
// fail fast if they run long, while explorer / ad-hoc queries may legitimately
// scan 10× more data.
type QueryProfile int

const (
	// ProfileOverview — defaults for dashboards / overview / infrastructure /
	// saturation / HTTP metrics / services. 15 s, 100M rows, 2 GB.
	ProfileOverview QueryProfile = iota
	// ProfileExplorer — defaults for ad-hoc explorer + tracedetail + ai
	// explorer + alerting backtest. 60 s, 1B rows, 8 GB.
	ProfileExplorer
)

// SettingsClauseForTest exposes the internal settings string for the
// tests/database package. Not intended for production callers — use
// SelectOverview / SelectExplorer.
func (p QueryProfile) SettingsClauseForTest() string { return p.settingsClause() }

func (p QueryProfile) settingsClause() string {
	switch p {
	case ProfileExplorer:
		return "SETTINGS max_execution_time=60, max_rows_to_read=1000000000, max_memory_usage=8589934592, max_result_rows=1000000, result_overflow_mode='break', read_overflow_mode='break', optimize_read_in_order=1"
	default:
		return "SETTINGS max_execution_time=15, max_rows_to_read=100000000, max_memory_usage=2147483648, max_result_rows=100000, result_overflow_mode='break', read_overflow_mode='break', optimize_read_in_order=1"
	}
}

func injectMaxExecutionTime(query string, profile QueryProfile) string {
	qUpper := strings.ToUpper(strings.TrimSpace(query))
	if (strings.HasPrefix(qUpper, "SELECT") || strings.HasPrefix(qUpper, "WITH")) && !strings.Contains(qUpper, "SETTINGS ") {
		return query + "\n" + profile.settingsClause()
	}
	return query
}

func optimizeQuery(query string, profile QueryProfile) string {
	query = injectPrewhere(query)
	query = injectMaxExecutionTime(query, profile)
	return query
}

// NativeQuerier wraps clickhouse.Conn with shared circuit-breaking, slow query
// logging, and ClickHouse-specific query rewrites. Repositories should prefer
// Select/QueryRow with typed structs for ClickHouse access.
type NativeQuerier struct {
	conn clickhouse.Conn
}

func NewNativeQuerier(conn clickhouse.Conn) *NativeQuerier {
	return &NativeQuerier{
		conn: conn,
	}
}

// isRetriableClickHouseNetworkError reports whether err is likely transient at the
// transport layer and safe to retry for read-only queries.
func isRetriableClickHouseNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var chErr *clickhouse.Exception
	if errors.As(err, &chErr) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	lower := strings.ToLower(err.Error())
	for _, marker := range clickHouseTransientErrorSubstrings {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

const clickHouseReadMaxAttempts = 3

// retryClickHouseRead re-executes op on transient connection failures (initial try
// plus up to clickHouseReadMaxAttempts-1 retries).
func retryClickHouseRead(ctx context.Context, op func() error) error {
	var lastErr error
	for attempt := range clickHouseReadMaxAttempts {
		if attempt > 0 {
			backoff := time.Duration(50*(1<<uint(attempt-1))) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			slog.Info("clickhouse_native retry after transient error",
				slog.Int("attempt", attempt+1),
				slog.String("cause", lastErr.Error()))
		}
		lastErr = op()
		if lastErr == nil {
			return nil
		}
		if !isRetriableClickHouseNetworkError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

// SelectOverview executes a SELECT with the overview (dashboard) budget —
// 15s execution, 100M rows, 2 GB memory. Use from overview/ infrastructure/
// saturation/ HTTP-metrics/ services repositories.
func (n *NativeQuerier) SelectOverview(ctx context.Context, dest any, query string, args ...any) error {
	return n.selectWithProfile(ctx, ProfileOverview, dest, query, args...)
}

// SelectExplorer executes a SELECT with the explorer (ad-hoc) budget —
// 60s execution, 1B rows, 8 GB memory. Use from explorer/ tracedetail/
// tracecompare/ ai-explorer/ alerting-backtest code paths.
func (n *NativeQuerier) SelectExplorer(ctx context.Context, dest any, query string, args ...any) error {
	return n.selectWithProfile(ctx, ProfileExplorer, dest, query, args...)
}

// QueryRowOverview is the single-row counterpart to SelectOverview.
func (n *NativeQuerier) QueryRowOverview(ctx context.Context, dest any, query string, args ...any) error {
	return n.queryRowWithProfile(ctx, ProfileOverview, dest, query, args...)
}

// QueryRowExplorer is the single-row counterpart to SelectExplorer.
func (n *NativeQuerier) QueryRowExplorer(ctx context.Context, dest any, query string, args ...any) error {
	return n.queryRowWithProfile(ctx, ProfileExplorer, dest, query, args...)
}

func (n *NativeQuerier) selectWithProfile(ctx context.Context, profile QueryProfile, dest any, query string, args ...any) error {
	query = optimizeQuery(query, profile)
	return retryClickHouseRead(ctx, func() error {
		return n.conn.Select(ctx, dest, query, args...)
	})
}

func (n *NativeQuerier) queryRowWithProfile(ctx context.Context, profile QueryProfile, dest any, query string, args ...any) error {
	query = optimizeQuery(query, profile)
	return retryClickHouseRead(ctx, func() error {
		return n.conn.QueryRow(ctx, query, args...).ScanStruct(dest)
	})
}

// Select is the unprofiled alias retained for the pre-Phase-4 call sites;
// it forwards to SelectOverview so the default budget is the cheaper one.
// Deprecated: pick SelectOverview or SelectExplorer explicitly at the call site.
func (n *NativeQuerier) Select(ctx context.Context, dest any, query string, args ...any) error {
	return n.SelectOverview(ctx, dest, query, args...)
}

// QueryRow is the unprofiled alias. See Select above.
// Deprecated: pick QueryRowOverview or QueryRowExplorer explicitly.
func (n *NativeQuerier) QueryRow(ctx context.Context, dest any, query string, args ...any) error {
	return n.QueryRowOverview(ctx, dest, query, args...)
}

// SelectTyped executes a SELECT query and scans results into a typed slice
// using the overview budget. For explorer / tracedetail / backtest paths use
// SelectTypedExplorer instead.
func SelectTyped[T any](ctx context.Context, db *NativeQuerier, query string, args ...any) ([]T, error) {
	var rows []T
	err := db.SelectOverview(ctx, &rows, query, args...)
	return rows, err
}

// SelectTypedExplorer is the explorer-budget counterpart to SelectTyped.
func SelectTypedExplorer[T any](ctx context.Context, db *NativeQuerier, query string, args ...any) ([]T, error) {
	var rows []T
	err := db.SelectExplorer(ctx, &rows, query, args...)
	return rows, err
}

// QueryRowTyped executes a single-row query with the overview budget.
func QueryRowTyped[T any](ctx context.Context, db *NativeQuerier, query string, args ...any) (T, error) {
	var row T
	err := db.QueryRowOverview(ctx, &row, query, args...)
	return row, err
}

// QueryRowTypedExplorer is the explorer-budget counterpart to QueryRowTyped.
func QueryRowTypedExplorer[T any](ctx context.Context, db *NativeQuerier, query string, args ...any) (T, error) {
	var row T
	err := db.QueryRowExplorer(ctx, &row, query, args...)
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
