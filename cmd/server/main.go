package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/observability/observability-backend-go/internal/config"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/server"
)

func main() {
	cfg := config.Load()

	dbConn, err := database.Open(cfg.MySQLDSN(), cfg.MaxMySQLOpenConns, cfg.MaxMySQLIdleConns)
	if err != nil {
		log.Fatalf("failed to connect mysql: %v", err)
	}
	defer dbConn.Close()

	chConn, err := database.OpenClickHouse(cfg.ClickHouseDSN(), cfg.ClickHouseProduction)
	if err != nil {
		log.Fatalf("failed to connect clickhouse: %v", err)
	}
	defer chConn.Close()

	// Native conn for ingest queues (PrepareBatch API).
	chNative, err := database.OpenClickHouseConn(cfg.ClickHouseDSN(), cfg.ClickHouseProduction)
	if err != nil {
		log.Fatalf("failed to open native clickhouse conn: %v", err)
	}
	defer chNative.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	app := server.New(dbConn, chConn, chNative, cfg)

	log.Printf("Server starting on port %s", cfg.Port)
	if err := app.Start(ctx); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
