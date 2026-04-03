// Package migrate runs goose database migrations for MySQL and ClickHouse.
// It is called on server boot to auto-apply pending migrations before
// the server starts serving traffic.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver registration
	_ "github.com/go-sql-driver/mysql"         // MySQL driver registration
	"github.com/pressly/goose/v3"

	"github.com/Optikk-Org/optikk-backend/internal/config"
)

// Run applies all pending migrations for both MySQL and ClickHouse.
// It uses the DSN and production flag from the loaded config.
// Goose is idempotent — already-applied migrations are skipped.
func Run(cfg config.Config) error {
	if err := runMySQL(cfg.MySQLDSN()); err != nil {
		return fmt.Errorf("mysql migrations: %w", err)
	}
	if err := runClickHouse(cfg.ClickHouseDSN(), cfg.ClickHouse.Production); err != nil {
		return fmt.Errorf("clickhouse migrations: %w", err)
	}
	return nil
}

func runMySQL(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	provider, err := goose.NewProvider(
		goose.DialectMySQL,
		db,
		os.DirFS("migrations/mysql"),
		goose.WithGoMigrations(
			goose.NewGoMigration(1, &goose.GoFunc{RunDB: mysqlUp001}, &goose.GoFunc{RunDB: mysqlDown001}),
			goose.NewGoMigration(2, &goose.GoFunc{RunDB: mysqlUp002}, &goose.GoFunc{RunDB: mysqlDown002}),
			goose.NewGoMigration(3, &goose.GoFunc{RunDB: mysqlUp003}, &goose.GoFunc{RunDB: mysqlDown003}),
			goose.NewGoMigration(4, &goose.GoFunc{RunDB: mysqlUp004}, &goose.GoFunc{RunDB: mysqlDown004}),
		),
	)
	if err != nil {
		return fmt.Errorf("provider: %w", err)
	}

	results, err := provider.Up(context.Background())
	if err != nil {
		return err
	}
	for _, r := range results {
		fmt.Printf("[migrate] mysql: applied %s (%s)\n", r.Source.Path, r.Duration)
	}
	return nil
}

func runClickHouse(dsn string, production bool) error {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	provider, err := goose.NewProvider(
		goose.DialectClickHouse,
		db,
		os.DirFS("migrations/clickhouse"),
		goose.WithGoMigrations(
			goose.NewGoMigration(1, &goose.GoFunc{RunDB: chUp001(production)}, &goose.GoFunc{RunDB: chDown001}),
		),
	)
	if err != nil {
		return fmt.Errorf("provider: %w", err)
	}

	results, err := provider.Up(context.Background())
	if err != nil {
		return err
	}
	for _, r := range results {
		fmt.Printf("[migrate] clickhouse: applied %s (%s)\n", r.Source.Path, r.Duration)
	}
	return nil
}
