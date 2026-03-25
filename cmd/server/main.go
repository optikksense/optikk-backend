package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/observability/observability-backend-go/internal/config"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/logger"
	"github.com/observability/observability-backend-go/internal/platform/server"
)

func main() {
	configPath := flag.String("config", "config.yml", "path to config file")
	flag.Parse()

	cfg := config.Load(*configPath)

	mode := "production"
	if os.Getenv("ENV") == "development" {
		mode = "development"
	}
	logger.Init(mode)
	defer logger.Sync()
	log := logger.L()

	dbConn, err := database.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		log.Fatal("failed to connect mysql", zap.Error(err))
	}
	defer dbConn.Close()

	chCloud := database.ClickHouseCloudConfig{
		Host:     cfg.ClickHouse.CloudHost,
		Username: cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
	}
	chConn, err := database.OpenClickHouseConn(cfg.ClickHouseDSN(), cfg.ClickHouse.Production, chCloud)
	if err != nil {
		log.Fatal("failed to connect clickhouse", zap.Error(err))
	}
	defer chConn.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	app := server.New(dbConn, chConn, cfg)

	log.Info("server starting", zap.String("port", cfg.Server.Port))
	if err := app.Start(ctx); err != nil {
		log.Fatal("server failed", zap.Error(err))
	}
}
