// Package logger provides structured logging via log/slog.
//
// Usage:
//
//	logger.Init("production")  // or "development"
//	logger.L().Info("server started", slog.Int("port", 8080))
//
// In request handlers, use the Gin middleware to inject a per-request logger
// with request_id into the context:
//
//	r.Use(logger.GinMiddleware())
//	// then in handler:
//	log := logger.FromCtx(c.Request.Context())
//	log.Info("handling request")
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lmittmann/tint"
)

var (
	global *slog.Logger
	once   sync.Once
)

type ctxKey struct{}

// Init initializes the global logger. Safe to call multiple times; only the
// first call takes effect. Pass "development" for pretty colored console output,
// anything else for JSON production output.
func Init(mode string) {
	once.Do(func() {
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
		if mode == "development" {
			handler = tint.NewHandler(os.Stdout, &tint.Options{
				Level:      level,
				TimeFormat: time.Kitchen,
				AddSource:  true,
			})
		} else {
			handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				Level:     level,
				AddSource: true,
			})
		}

		global = slog.New(handler)
		slog.SetDefault(global)
	})
}

// L returns the global logger. If Init hasn't been called, returns the slog default.
func L() *slog.Logger {
	if global == nil {
		return slog.Default()
	}
	return global
}

// WithCtx returns a new context with the given logger attached.
func WithCtx(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromCtx extracts the logger from context, or returns the global logger.
func FromCtx(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return L()
}

// Sync is a no-op retained for API compatibility. slog writes are unbuffered.
func Sync() {}
