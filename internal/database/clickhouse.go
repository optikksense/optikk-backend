package database

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	circuitbreaker "github.com/observability/observability-backend-go/internal/platform/circuit_breaker"
)

const slowQueryThreshold = 100 * time.Millisecond

func truncateQuery(q string) string {
	q = strings.TrimSpace(q)
	if len(q) > 200 {
		return q[:200] + "..."
	}
	return q
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
			return nil, fmt.Errorf("clickhouse: ClickHouseCloudConfig required for production mode")
		}
		cc := cloud[0]
		opts = &clickhouse.Options{
			Addr:     []string{cc.Host},
			Protocol: clickhouse.Native,
			TLS:      &tls.Config{},
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

type circuitBreakerRowAdapter struct {
	err error
}

func (r *circuitBreakerRowAdapter) Scan(dest ...any) error {
	return r.err
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
	r.rows.Scan(dest...)
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
	cb   *circuitbreaker.CircuitBreaker
}

func NewNativeQuerier(conn clickhouse.Conn) *NativeQuerier {
	return &NativeQuerier{
		conn: conn,
		cb:   circuitbreaker.NewCircuitBreakerWithSuccessDecider("clickhouse_native", 5, 30*time.Second, isClickHouseQuerySuccessfulForBreaker),
	}
}

func isClickHouseQuerySuccessfulForBreaker(err error) bool {
	if err == nil {
		return true
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return false
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return false
	}

	var chErr *clickhouse.Exception
	if errors.As(err, &chErr) {
		return true
	}

	lower := strings.ToLower(err.Error())
	for _, marker := range []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"i/o timeout",
		"no such host",
		"timeout exceeded",
		"connection closed",
		"unexpected eof",
	} {
		if strings.Contains(lower, marker) {
			return false
		}
	}

	return true
}

func (n *NativeQuerier) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = optimizeQuery(query)
	start := time.Now()
	err := n.cb.Call(func() error {
		return n.conn.Select(ctx, dest, query, args...)
	})
	if d := time.Since(start); d >= slowQueryThreshold {
		log.Printf("SLOW_QUERY clickhouse_native duration=%v query=%s", d, truncateQuery(query))
	}
	return err
}

func (n *NativeQuerier) QueryRow(ctx context.Context, dest any, query string, args ...any) error {
	query = optimizeQuery(query)
	start := time.Now()
	err := n.cb.Call(func() error {
		return n.conn.QueryRow(ctx, query, args...).ScanStruct(dest)
	})
	if d := time.Since(start); d >= slowQueryThreshold {
		log.Printf("SLOW_QUERY clickhouse_native duration=%v query=%s", d, truncateQuery(query))
	}
	return err
}
