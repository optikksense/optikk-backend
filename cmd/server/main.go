package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lmittmann/tint"

	"github.com/Optikk-Org/optikk-backend/internal/app/server"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
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
	initLogger(mode)

	dbConn, err := database.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		slog.Error("failed to connect mysql", slog.Any("error", err))
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
		slog.Error("failed to connect clickhouse", slog.Any("error", err))
		os.Exit(1)
	}
	defer chConn.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	app, err := server.New(dbConn, chConn, cfg)
	if err != nil {
		slog.Error("failed to initialize app", slog.Any("error", err))
		os.Exit(1)
	}

	if err := app.Start(ctx); err != nil {
		slog.Error("server failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func initLogger(mode string) {
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)

	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
		switch strings.ToLower(lvl) {
		case "debug":
			level.Set(slog.LevelDebug)
		case "info":
			level.Set(slog.LevelInfo)
		case "warn", "warning":
			level.Set(slog.LevelWarn)
		case "error":
			level.Set(slog.LevelError)
		}
	}

	var handler slog.Handler
	format := strings.ToLower(os.Getenv("LOG_FORMAT"))

	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:     level,
			AddSource: true,
		})
	} else {
		handler = tint.NewHandler(os.Stdout, &tint.Options{
			Level:      level,
			TimeFormat: time.Kitchen,
			AddSource:  true,
		})
	}

	slog.SetDefault(slog.New(handler))
}
