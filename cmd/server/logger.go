package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/lmittmann/tint"
)

func initLogger() {
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
