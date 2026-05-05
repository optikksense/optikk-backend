package main

import (
	"log/slog"
	"os"
)

func initLogger() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})
	slog.SetDefault(slog.New(handler))
}
