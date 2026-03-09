package database

import (
	"context"
	"crypto/tls"
	"database/sql"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	circuitbreaker "github.com/observability/observability-backend-go/internal/platform/circuit_breaker"
)

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

func OpenClickHouse(dsn string, isProduction bool) (*sql.DB, error) {
	var conn *sql.DB
	var err error

	if isProduction {
		conn = clickhouse.OpenDB(&clickhouse.Options{
			Addr:     []string{"e4q81bjqva.us-central1.gcp.clickhouse.cloud:9440"},
			Protocol: clickhouse.Native,
			TLS:      &tls.Config{},
			Auth: clickhouse.Auth{
				Username: "default",
				Password: "CHZoMiHfX_5vt",
			},
			DialTimeout: 5 * time.Second,
			ReadTimeout: 30 * time.Second,
		})
	} else {
		conn, err = sql.Open("clickhouse", dsn)
		if err != nil {
			return nil, err
		}
	}

	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(25)
	conn.SetConnMaxLifetime(15 * time.Minute)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := conn.PingContext(pingCtx); err != nil {
		return nil, err
	}

	return conn, nil
}

type ClickHouseWrapper struct {
	db *sql.DB
	cb *circuitbreaker.CircuitBreaker
}

func NewClickHouseWrapper(db *sql.DB) *ClickHouseWrapper {
	return &ClickHouseWrapper{
		db: db,
		cb: circuitbreaker.NewCircuitBreaker("clickhouse_wrapper", 5, 30*time.Second),
	}
}

// prewhereRe matches: WHERE <alias>.team_id = ? AND <alias>.ts_bucket_start BETWEEN ? AND ?
// and rewrites the team_id + ts_bucket_start conditions into a PREWHERE clause,
// keeping the remaining conditions in WHERE. This lets ClickHouse skip granules
// before decompressing columns, giving 2-10x speedup on large tables.
var prewhereRe = regexp.MustCompile(
	`(?i)\bWHERE\s+([\w.]*\.?)team_id\s*=\s*\?\s+AND\s+([\w.]*\.?)ts_bucket_start\s+BETWEEN\s+\?\s+AND\s+\?\s+AND\s+`,
)

func injectPrewhere(query string) string {
	if strings.Contains(strings.ToUpper(query), "PREWHERE") {
		return query // already has PREWHERE
	}
	// Rewrite: WHERE a.team_id = ? AND a.ts_bucket_start BETWEEN ? AND ? AND <rest>
	//      to: PREWHERE a.team_id = ? AND a.ts_bucket_start BETWEEN ? AND ? WHERE <rest>
	return prewhereRe.ReplaceAllString(query, "PREWHERE ${1}team_id = ? AND ${2}ts_bucket_start BETWEEN ? AND ? WHERE ")
}

func injectMaxExecutionTime(query string) string {
	qUpper := strings.ToUpper(strings.TrimSpace(query))
	if (strings.HasPrefix(qUpper, "SELECT") || strings.HasPrefix(qUpper, "WITH")) && !strings.Contains(qUpper, "SETTINGS ") {
		return query + "\nSETTINGS max_execution_time=30, max_rows_to_read=500000000, read_overflow_mode='break', optimize_read_in_order=1"
	}
	return query
}

// optimizeQuery applies all query rewrites: PREWHERE injection + SETTINGS injection.
func optimizeQuery(query string) string {
	query = injectPrewhere(query)
	query = injectMaxExecutionTime(query)
	return query
}

func (m *ClickHouseWrapper) Exec(query string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := m.cb.Call(func() error {
		var err error
		res, err = m.db.Exec(query, args...)
		return err
	})
	return res, err
}

func (m *ClickHouseWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := m.cb.Call(func() error {
		var err error
		res, err = m.db.ExecContext(ctx, query, args...)
		return err
	})
	return res, err
}

func (m *ClickHouseWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return m.db.BeginTx(ctx, opts)
}

func (m *ClickHouseWrapper) Query(query string, args ...any) (Rows, error) {
	query = optimizeQuery(query)
	var rows *sql.Rows
	err := m.cb.Call(func() error {
		var err error
		rows, err = m.db.Query(query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &sqlRowsAdapter{rows: rows}, nil
}

func (m *ClickHouseWrapper) QueryRow(query string, args ...any) Row {
	query = optimizeQuery(query)
	var row *sql.Row
	// We run QueryRow under the circuit breaker, but since it doesn't return an error directly
	// (errors are deferred to Scan), any immediate errors (like connection drops) might not drop the breaker here.
	// However, if the circuit is currently open, we return a structural dummy that returns ErrCircuitOpen on Scan.
	err := m.cb.Call(func() error {
		row = m.db.QueryRow(query, args...)
		return nil // QueryRow doesn't return an error itself
	})
	if err != nil {
		return &circuitBreakerRowAdapter{err: err}
	}
	return &sqlRowAdapter{row: row}
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

func (m *ClickHouseWrapper) Close() error {
	return m.db.Close()
}
