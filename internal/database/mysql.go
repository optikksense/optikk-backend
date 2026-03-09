package database

import (
	"context"
	"database/sql"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// injectMySQLTimeouts adds connection timeouts to the DSN if not already present.
// This prevents indefinite hangs on network issues.
func injectMySQLTimeouts(dsn string) string {
	timeouts := map[string]string{
		"timeout":      "5s",
		"readTimeout":  "30s",
		"writeTimeout":  "30s",
	}
	for param, val := range timeouts {
		if !strings.Contains(dsn, param+"=") {
			sep := "&"
			if !strings.Contains(dsn, "?") {
				sep = "?"
			}
			dsn = dsn + sep + param + "=" + val
		}
	}
	return dsn
}

// Open creates a MySQL connection pool. maxOpen and maxIdle control pool size;
// pass 0 to use defaults (50 open, 25 idle).
// Automatically injects connection timeouts into the DSN if not already present.
func Open(dsn string, maxOpen, maxIdle int) (*sql.DB, error) {
	dsn = injectMySQLTimeouts(dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if maxOpen <= 0 {
		maxOpen = 50
	}
	if maxIdle <= 0 {
		maxIdle = maxOpen / 2
	}

	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		return nil, err
	}

	return db, nil
}

type MySQLWrapper struct {
	db *sql.DB
}

func NewMySQLWrapper(db *sql.DB) *MySQLWrapper {
	return &MySQLWrapper{
		db: db,
	}
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
	row := m.db.QueryRow(query, args...)
	return &sqlRowAdapter{row: row}
}

func (m *MySQLWrapper) Close() error {
	return m.db.Close()
}
