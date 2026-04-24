// Command migrate applies ClickHouse DDL from db/clickhouse/*.sql in lexical
// order, skipping already-applied files via the
// observability.schema_migrations tracking table.
//
// Usage:
//
//	migrate up       # apply all pending migrations
//	migrate status   # print applied / pending list
//
// Connection settings come from config.yml (resolved the same way the server
// does). Optional `--config <path>` overrides.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	chembed "github.com/Optikk-Org/optikk-backend/db/clickhouse"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database_chmigrate"
)

func main() {
	configPath := flag.String("config", "", "path to config.yml (default: auto-resolve like server)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usageAndExit()
	}
	cmd := args[0]

	initLogger()

	var cfg config.Config
	var err error
	if *configPath != "" {
		cfg, err = config.Load(*configPath)
	} else {
		cfg, err = config.Load()
	}
	if err != nil {
		slog.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}

	conn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN())
	if err != nil {
		slog.Error("connect clickhouse", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	m := &chmigrate.Migrator{
		DB:       conn,
		FS:       chembed.FS,
		Database: cfg.ClickHouse.Database,
		Logger:   func(format string, args ...any) { slog.Info(fmt.Sprintf(format, args...)) },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	switch cmd {
	case "up":
		applied, skipped, err := m.Up(ctx)
		if err != nil {
			slog.Error("migrate up failed", slog.Any("error", err))
			os.Exit(1)
		}
		slog.Info("migrate up complete", slog.Int("applied", applied), slog.Int("skipped", skipped))
	case "status":
		rows, err := m.Status(ctx)
		if err != nil {
			slog.Error("migrate status failed", slog.Any("error", err))
			os.Exit(1)
		}
		applied, pending := 0, 0
		for _, r := range rows {
			mark := "pending"
			if r.Applied {
				mark = "applied"
				applied++
			} else {
				pending++
			}
			fmt.Printf("%s  %s\n", mark, r.Version)
		}
		fmt.Printf("\n%d applied, %d pending\n", applied, pending)
	default:
		usageAndExit()
	}
}

func usageAndExit() {
	fmt.Fprintln(os.Stderr, "usage: migrate [--config <path>] <up|status>")
	os.Exit(2)
}

func initLogger() {
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(handler))
}
