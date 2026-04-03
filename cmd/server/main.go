package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Optikk-Org/optikk-backend/internal/app/server"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/Optikk-Org/optikk-backend/internal/infra/migrate"
)

func main() {
	configPath := flag.String("config", "config.yml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	mode := "production"
	if os.Getenv("ENV") == "development" {
		mode = "development"
	}
	logger.Init(mode)
	defer logger.Sync()
	log := logger.L()

	dbConn, err := database.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		log.Error("failed to connect mysql", slog.Any("error", err))
		os.Exit(1)
	}
	defer dbConn.Close()

	chCloud := database.ClickHouseCloudConfig{
		Host:     cfg.ClickHouse.CloudHost,
		Username: cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
	}
	chConn, err := database.OpenClickHouseConn(cfg.ClickHouseDSN(), cfg.ClickHouse.Production, chCloud)
	if err != nil {
		log.Error("failed to connect clickhouse", slog.Any("error", err))
		os.Exit(1)
	}
	defer chConn.Close()

	// Auto-apply pending database migrations before serving traffic.
	log.Info("running database migrations")
	if err := migrate.Run(cfg); err != nil {
		log.Error("migration failed", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	app, err := server.New(dbConn, chConn, cfg)
	if err != nil {
		log.Error("failed to initialize app", slog.Any("error", err))
		os.Exit(1)
	}

	log.Info("server starting", slog.String("port", cfg.Server.Port))
	if err := app.Start(ctx); err != nil {
		log.Error("server failed", slog.Any("error", err))
		os.Exit(1)
	}
}
