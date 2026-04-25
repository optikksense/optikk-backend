package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/shared/slogx"
	"github.com/lmittmann/tint"
)

// initLogger builds the app's root slog handler. A FanoutHandler wraps a
// single stdout leg today; new sinks (file, syslog) can be added by
// appending handlers without touching call sites.
func initLogger() {
	level := resolveLevel()
	stdout := resolveStdoutHandler(level)
	root := slogx.NewFanoutHandler(stdout)
	slog.SetDefault(slog.New(root))
}

func resolveLevel() *slog.LevelVar {
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	lvl := strings.ToLower(os.Getenv("LOG_LEVEL"))
	switch lvl {
	case "debug":
		level.Set(slog.LevelDebug)
	case "info", "":
		level.Set(slog.LevelInfo)
	case "warn", "warning":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	}
	return level
}

func resolveStdoutHandler(level *slog.LevelVar) slog.Handler {
	format := strings.ToLower(os.Getenv("LOG_FORMAT"))
	if format == "json" {
		return slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:     level,
			AddSource: true,
		})
	}
	return tint.NewHandler(os.Stdout, &tint.Options{
		Level:      level,
		TimeFormat: time.Kitchen,
		AddSource:  true,
	})
}
