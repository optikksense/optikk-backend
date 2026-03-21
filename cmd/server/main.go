package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/observability/observability-backend-go/internal/config"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/server"
)

func main() {
	configPath := flag.String("config", "config.yml", "path to config file")
	flag.Parse()

	cfg := config.Load(*configPath)

	dbConn, err := database.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		log.Fatalf("failed to connect mysql: %v", err)
	}
	defer dbConn.Close()

	chCloud := database.ClickHouseCloudConfig{
		Host:     cfg.ClickHouse.CloudHost,
		Username: cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
	}
	chConn, err := database.OpenClickHouseConn(cfg.ClickHouseDSN(), cfg.ClickHouse.Production, chCloud)
	if err != nil {
		log.Fatalf("failed to connect clickhouse: %v", err)
	}
	defer chConn.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	app := server.New(dbConn, chConn, cfg)

	log.Printf("Server starting on port %s", cfg.Server.Port)
	if err := app.Start(ctx); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
