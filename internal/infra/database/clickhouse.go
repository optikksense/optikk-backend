package database

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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

type Querier interface {
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (Rows, error)
	QueryRow(query string, args ...any) Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Close() error
}

type Rows interface {
	Columns() []string
	Close() error
	Next() bool
	Scan(dest ...any)
}

type Row interface {
	Scan(dest ...any) error
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

type sqlRowsAdapter struct {
	rows *sql.Rows
}

func (r *sqlRowsAdapter) Columns() []string {
	cols, _ := r.rows.Columns()
	return cols
}

func (r *sqlRowsAdapter) Close() error {
	return r.rows.Close()
}

func (r *sqlRowsAdapter) Next() bool {
	return r.rows.Next()
}

func (r *sqlRowsAdapter) Scan(dest ...any) {
	_ = r.rows.Scan(dest...)
}

type sqlRowAdapter struct {
	row *sql.Row
}

func (r *sqlRowAdapter) Scan(dest ...any) error {
	return r.row.Scan(dest...)
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
// plus up to clickHouseReadMaxAttempts-1 retries). Callers wrap this inside
// circuitbreaker.Call: transient failures on earlier attempts do not trip the
// breaker; only the final returned error counts toward breaker state.
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

func (n *NativeQuerier) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = optimizeQuery(query)
	return retryClickHouseRead(ctx, func() error {
		return n.conn.Select(ctx, dest, query, args...)
	})
}

func (n *NativeQuerier) QueryRow(ctx context.Context, dest any, query string, args ...any) error {
	query = optimizeQuery(query)
	return retryClickHouseRead(ctx, func() error {
		return n.conn.QueryRow(ctx, query, args...).ScanStruct(dest)
	})
}

// SelectTyped executes a SELECT query and scans results into a typed slice.
// Eliminates the common three-line pattern: var rows []T; err := db.Select(...); return rows, err
func SelectTyped[T any](ctx context.Context, db *NativeQuerier, query string, args ...any) ([]T, error) {
	var rows []T
	err := db.Select(ctx, &rows, query, args...)
	return rows, err
}

// QueryRowTyped executes a single-row query and scans into a typed struct.
func QueryRowTyped[T any](ctx context.Context, db *NativeQuerier, query string, args ...any) (T, error) {
	var row T
	err := db.QueryRow(ctx, &row, query, args...)
	return row, err
}
