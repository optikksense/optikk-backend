package main

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/shared/slogx"
	"github.com/lmittmann/tint"
)

// initLogger builds the app's root slog handler:
//
//	TraceAttrHandler            — injects trace_id / span_id from ctx
//	└─ FanoutHandler
//	   ├─ stdout (tint / JSON)  — human-readable local dev + kubectl logs
//	   └─ OTel logs bridge      — ships each record to the otel-collector → Loki
//
// The bridge leg is attached lazily so it picks up whatever logger
// provider `internal/infra/otel` installs at app boot. If OTel is
// disabled in config the bridge is a cheap no-op (global.GetLoggerProvider
// returns a noop provider) so there is no separate code path.
func initLogger() {
	level := resolveLevel()
	stdout := resolveStdoutHandler(level)
	bridge := slogx.NewOtelBridgeHandler("optikk-backend")

	fan := slogx.NewFanoutHandler(stdout, bridge)
	root := slogx.NewTraceAttrHandler(fan)
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
