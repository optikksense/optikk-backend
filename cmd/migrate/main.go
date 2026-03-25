// Command migrate runs goose database migrations for MySQL or ClickHouse.
//
// Usage:
//
//	go run ./cmd/migrate -db mysql -dsn "user:pass@tcp(host:3306)/dbname" up
//	go run ./cmd/migrate -db clickhouse -dsn "clickhouse://user:pass@host:9000/dbname" up
//	go run ./cmd/migrate -db mysql -dsn "..." status
//	go run ./cmd/migrate -db mysql -dsn "..." down
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"
)

func main() {
	dbType := flag.String("db", "mysql", "database type: mysql or clickhouse")
	dsn := flag.String("dsn", "", "database connection string")
	flag.Parse()

	if *dsn == "" {
		*dsn = os.Getenv("MIGRATE_DSN")
	}
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "error: -dsn flag or MIGRATE_DSN env required")
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "error: command required (up, down, status, create)")
		os.Exit(1)
	}
	command := args[0]

	var driver, migrationsDir string
	switch *dbType {
	case "mysql":
		driver = "mysql"
		migrationsDir = "migrations/mysql"
	case "clickhouse":
		driver = "clickhouse"
		migrationsDir = "migrations/clickhouse"
	default:
		fmt.Fprintf(os.Stderr, "error: unsupported db type: %s\n", *dbType)
		os.Exit(1)
	}

	db, err := sql.Open(driver, *dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := goose.SetDialect(driver); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to set dialect: %v\n", err)
		os.Exit(1)
	}

	if err := goose.Run(command, db, migrationsDir, args[1:]...); err != nil {
		fmt.Fprintf(os.Stderr, "error: goose %s: %v\n", command, err)
		os.Exit(1)
	}
}
