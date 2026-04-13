package database

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver registration
	"github.com/jmoiron/sqlx"
)

// injectMySQLTimeouts adds connection timeouts to the DSN if not already present.
// This prevents indefinite hangs on network issues.
func injectMySQLTimeouts(dsn string) string {
	timeouts := map[string]string{
		"timeout":      "5s",
		"readTimeout":  "30s",
		"writeTimeout": "30s",
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
	db *sqlx.DB
}

func NewMySQLWrapper(db *sql.DB) *MySQLWrapper {
	return &MySQLWrapper{
		db: sqlx.NewDb(db, "mysql"),
	}
}

func (m *MySQLWrapper) Exec(query string, args ...any) (sql.Result, error) {
	return m.ExecContext(context.Background(), query, args...)
}

func (m *MySQLWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	q, a, err := expandArgs(query, args...)
	if err != nil {
		return nil, err
	}
	return m.db.ExecContext(ctx, q, a...)
}

func (m *MySQLWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return m.db.BeginTx(ctx, opts)
}

func (m *MySQLWrapper) Query(query string, args ...any) (Rows, error) {
	q, a, err := expandArgs(query, args...)
	if err != nil {
		return nil, err
	}
	rows, err := m.db.QueryContext(context.Background(), q, a...)
	if err != nil {
		return nil, err
	}
	return &sqlRowsAdapter{rows: rows}, nil
}

func (m *MySQLWrapper) QueryRow(query string, args ...any) Row {
	q, a, err := expandArgs(query, args...)
	if err != nil {
		return &errorRowAdapter{err: err}
	}
	row := m.db.QueryRowContext(context.Background(), q, a...)
	return &sqlRowAdapter{row: row}
}

func (m *MySQLWrapper) Close() error {
	return m.db.Close()
}

func expandArgs(query string, args ...any) (string, []any, error) {
	hasSlice := false
	for _, arg := range args {
		if arg == nil {
			continue
		}
		t := reflect.TypeOf(arg)
		if t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 {
			hasSlice = true
			break
		}
	}

	if !hasSlice {
		return query, args, nil
	}

	return sqlx.In(query, args...)
}

type errorRowAdapter struct {
	err error
}

func (e *errorRowAdapter) Scan(dest ...any) error {
	return e.err
}
