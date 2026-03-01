package main

// @title Observability Backend API
// @version 1.0
// @description Go/Gin backend for the observability platform. Handles REST API queries, JWT auth, multi-tenant telemetry ingestion (OTLP/HTTP), and ClickHouse persistence.
// @termsOfService http://swagger.io/terms/
// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @host localhost:8080
// @BasePath /
// @schemes http https
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description "Bearer {token}"

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

	chConn, err := database.OpenClickHouse(cfg.ClickHouseDSN())
	if err != nil {
		log.Fatalf("failed to connect clickhouse: %v", err)
	}
	defer chConn.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	app := server.New(dbConn, chConn, cfg)

	log.Printf("Server starting on port %s", cfg.Port)
	if err := app.Start(ctx); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
