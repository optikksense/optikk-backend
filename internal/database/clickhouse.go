package database

import (
	"context"
	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go/v2"
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

	if err := conn.PingContext(context.Background()); err != nil {
		return nil, err
	}

	return conn, nil
}

type MySQLWrapper struct {
	db *sql.DB
}

func NewMySQLWrapper(db *sql.DB) *MySQLWrapper {
	return &MySQLWrapper{db: db}
}

func (m *MySQLWrapper) Exec(query string, args ...any) (sql.Result, error) {
	return m.db.Exec(query, args...)
}

func (m *MySQLWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return m.db.ExecContext(ctx, query, args...)
}

func (m *MySQLWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return m.db.BeginTx(ctx, opts)
}

func (m *MySQLWrapper) Query(query string, args ...any) (Rows, error) {
	rows, err := m.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &sqlRowsAdapter{rows: rows}, nil
}

func (m *MySQLWrapper) QueryRow(query string, args ...any) Row {
	return &sqlRowAdapter{row: m.db.QueryRow(query, args...)}
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
