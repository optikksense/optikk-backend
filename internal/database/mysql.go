package database

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Open creates a MySQL connection pool. maxOpen and maxIdle control pool size;
// pass 0 to use defaults (50 open, 25 idle).
func Open(dsn string, maxOpen, maxIdle int) (*sql.DB, error) {
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

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

