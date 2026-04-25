package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Optikk-Org/optikk-backend/internal/app/server"
	"github.com/Optikk-Org/optikk-backend/internal/config"
)

func main() {
	initLogger()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop() // restore default SIGINT behavior so a second Ctrl+C kills the process
	}()

	app, err := server.New(cfg)
	if err != nil {
		slog.Error("failed to initialize app", slog.Any("error", err))
		os.Exit(1)
	}

	if err := app.Start(ctx); err != nil {
		slog.Error("server failed", slog.Any("error", err))
		os.Exit(1)
	}
}
