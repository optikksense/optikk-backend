package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// isIdempotentSchemaError returns true for MySQL errors that indicate a
// schema statement was already applied (duplicate column / duplicate key).
func isIdempotentSchemaError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Error 1060") ||
		strings.Contains(msg, "Duplicate column name") ||
		strings.Contains(msg, "Error 1061") ||
		strings.Contains(msg, "Duplicate key name")
}

func Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(3 * time.Minute)
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func RunSchema(db *sql.DB, root string) error {
	schemaPath := filepath.Join(root, "db", "schema.sql")
	sqlBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}

	content := string(sqlBytes)
	stmts := strings.Split(content, ";")
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			if isIdempotentSchemaError(err) {
				continue
			}
			return fmt.Errorf("exec schema stmt failed: %w; stmt=%s", err, stmt)
		}
	}
	log.Println("schema migration applied")
	return nil
}
