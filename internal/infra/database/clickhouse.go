package database

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// ---------------------------------------------------------------------------
// Connection pool defaults
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Connection opening
// ---------------------------------------------------------------------------

type ClickHouseCloudConfig struct {
	Host     string
	Username string
	Password string
}

func cloudOptions(cc ClickHouseCloudConfig) *clickhouse.Options {
	return &clickhouse.Options{
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
}

func OpenClickHouseConn(dsn string, isProduction bool, cloud ...ClickHouseCloudConfig) (clickhouse.Conn, error) {
	opts, err := resolveClickHouseOptions(dsn, isProduction, cloud)
	if err != nil {
		return nil, err
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

func resolveClickHouseOptions(dsn string, isProduction bool, cloud []ClickHouseCloudConfig) (*clickhouse.Options, error) {
	if isProduction {
		if len(cloud) == 0 {
			return nil, errors.New("clickhouse: ClickHouseCloudConfig required for production mode")
		}
		return cloudOptions(cloud[0]), nil
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse DSN: %w", err)
	}
	return opts, nil
}

// ---------------------------------------------------------------------------
// Query optimisation rewrites
// ---------------------------------------------------------------------------

var prewhereRe = regexp.MustCompile(
	`(?i)\bWHERE\s+([\w.]*\.?)team_id\s*=\s*\?\s+AND\s+([\w.]*\.?)ts_bucket_start\s+BETWEEN\s+\?\s+AND\s+\?\s+AND\s+`,
)

func injectPrewhere(query string) string {
	if strings.Contains(strings.ToUpper(query), "PREWHERE") {
		return query
	}
	return prewhereRe.ReplaceAllString(query, "PREWHERE ${1}team_id = ? AND ${2}ts_bucket_start BETWEEN ? AND ? WHERE ")
}

func injectMaxExecutionTime(query string) string {
	qUpper := strings.ToUpper(strings.TrimSpace(query))
	if (strings.HasPrefix(qUpper, "SELECT") || strings.HasPrefix(qUpper, "WITH")) && !strings.Contains(qUpper, "SETTINGS ") {
		return query + "\nSETTINGS max_execution_time=30, max_rows_to_read=100000000, max_result_rows=100000, result_overflow_mode='break', read_overflow_mode='break', optimize_read_in_order=1"
	}
	return query
}

func optimizeQuery(query string) string {
	query = injectPrewhere(query)
	query = injectMaxExecutionTime(query)
	return query
}

// ---------------------------------------------------------------------------
// NativeQuerier — read path (SELECT / QueryRow)
// ---------------------------------------------------------------------------

// NativeQuerier wraps clickhouse.Conn with shared circuit-breaking, slow query
// logging, and ClickHouse-specific query rewrites. Repositories should prefer
// Select/QueryRow with typed structs for ClickHouse access.
type NativeQuerier struct {
	conn clickhouse.Conn
}

func NewNativeQuerier(conn clickhouse.Conn) *NativeQuerier {
	return &NativeQuerier{conn: conn}
}

func (n *NativeQuerier) Select(ctx context.Context, dest any, query string, args ...any) error {
	return n.conn.Select(ctx, dest, optimizeQuery(query), args...)
}

func (n *NativeQuerier) QueryRow(ctx context.Context, dest any, query string, args ...any) error {
	return n.conn.QueryRow(ctx, optimizeQuery(query), args...).ScanStruct(dest)
}

// ---------------------------------------------------------------------------
// CHFlusher — write path (batch INSERT)
// ---------------------------------------------------------------------------

// CHFlusher batches rows and inserts them into a ClickHouse table using PrepareBatch.
type CHFlusher[T any] struct {
	conn        clickhouse.Conn
	queryPrefix string
	table       string
}

func NewCHFlusher[T any](conn clickhouse.Conn, table string, columns []string) *CHFlusher[T] {
	return &CHFlusher[T]{
		conn:        conn,
		queryPrefix: "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ")",
		table:       table,
	}
}

func (f *CHFlusher[T]) Flush(batch []T) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := f.conn.PrepareBatch(ctx, f.queryPrefix)
	if err != nil {
		slog.Error("ingest: prepare failed", slog.String("table", f.table), slog.Any("error", err))
		return err
	}
	for i, row := range batch {
		if err := b.AppendStruct(row); err != nil {
			slog.Error("ingest: append failed", slog.String("table", f.table), slog.Int("index", i), slog.Any("error", err))
			return err
		}
	}
	if err := b.Send(); err != nil {
		slog.Error("ingest: send failed", slog.String("table", f.table), slog.Any("error", err))
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Named parameter helpers
// ---------------------------------------------------------------------------

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
