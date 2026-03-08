package database

import (
	"context"
	"database/sql"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
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

func OpenClickHouse(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(25)
	conn.SetConnMaxLifetime(15 * time.Minute)

	if err := conn.PingContext(context.Background()); err != nil {
		return nil, err
	}

	return conn, nil
}

type MySQLWrapper struct {
	db *sql.DB
	cb *circuitbreaker.CircuitBreaker
}

func NewMySQLWrapper(db *sql.DB) *MySQLWrapper {
	return &MySQLWrapper{
		db: db,
		cb: circuitbreaker.NewCircuitBreaker("mysql_wrapper", 5, 30*time.Second),
	}
}

func injectMaxExecutionTime(query string) string {
	qUpper := strings.ToUpper(strings.TrimSpace(query))
	if (strings.HasPrefix(qUpper, "SELECT") || strings.HasPrefix(qUpper, "WITH")) && !strings.Contains(qUpper, "SETTINGS ") {
		return query + "\nSETTINGS max_execution_time=30"
	}
	return query
}

func (m *MySQLWrapper) Exec(query string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := m.cb.Call(func() error {
		var err error
		res, err = m.db.Exec(query, args...)
		return err
	})
	return res, err
}

func (m *MySQLWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := m.cb.Call(func() error {
		var err error
		res, err = m.db.ExecContext(ctx, query, args...)
		return err
	})
	return res, err
}

func (m *MySQLWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return m.db.BeginTx(ctx, opts)
}

func (m *MySQLWrapper) Query(query string, args ...any) (Rows, error) {
	query = injectMaxExecutionTime(query)
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

func (m *MySQLWrapper) QueryRow(query string, args ...any) Row {
	query = injectMaxExecutionTime(query)
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

func (m *MySQLWrapper) Close() error {
	return m.db.Close()
}
