package database

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"time"

	circuitbreaker "github.com/Optikk-Org/optikk-backend/internal/infra/circuitbreaker"
	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	_ "github.com/go-sql-driver/mysql" // MySQL driver registration
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
	db *sql.DB
	cb *circuitbreaker.CircuitBreaker
}

func NewMySQLWrapper(db *sql.DB, consecutiveFailures int, resetTimeout time.Duration) *MySQLWrapper {
	return &MySQLWrapper{
		db: db,
		cb: circuitbreaker.NewCircuitBreaker("mysql_wrapper", consecutiveFailures, resetTimeout),
	}
}

func (m *MySQLWrapper) Exec(query string, args ...any) (sql.Result, error) {
	var res sql.Result
	err := m.cb.Call(func() error {
		var err error
		res, err = m.db.ExecContext(context.Background(), query, args...)
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
	start := time.Now()
	var rows *sql.Rows
	err := m.cb.Call(func() error {
		var err error
		rows, err = m.db.QueryContext(context.Background(), query, args...) //nolint:rowserrcheck,sqlclosecheck // rows returned to caller via adapter
		return err
	})
	if d := time.Since(start); d >= slowQueryThreshold {
		logger.L().Warn("SLOW_QUERY mysql", slog.Duration("duration", d), slog.String("query", truncateQuery(query)))
	}
	if err != nil {
		return nil, err
	}
	return &sqlRowsAdapter{rows: rows}, nil
}

func (m *MySQLWrapper) QueryRow(query string, args ...any) Row {
	start := time.Now()
	var row *sql.Row
	err := m.cb.Call(func() error {
		row = m.db.QueryRowContext(context.Background(), query, args...)
		return nil
	})
	if d := time.Since(start); d >= slowQueryThreshold {
		logger.L().Warn("SLOW_QUERY mysql", slog.Duration("duration", d), slog.String("query", truncateQuery(query)))
	}
	if err != nil {
		return &circuitBreakerRowAdapter{err: err}
	}
	return &sqlRowAdapter{row: row}
}

func (m *MySQLWrapper) Close() error {
	return m.db.Close()
}
