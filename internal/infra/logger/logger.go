// Package logger provides structured logging via uber-go/zap.
//
// Usage:
//
//	logger.Init("production")  // or "development"
//	logger.L().Info("server started", zap.Int("port", 8080))
//	logger.S().Infof("listening on :%d", port)
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
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	global *zap.Logger
	once   sync.Once
)

type ctxKey struct{}

// Init initializes the global logger. Safe to call multiple times; only the
// first call takes effect. Pass "development" for pretty console output,
// anything else for JSON production output.
func Init(mode string) {
	once.Do(func() {
		var cfg zap.Config
		if mode == "development" {
			cfg = zap.NewDevelopmentConfig()
			cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		} else {
			cfg = zap.NewProductionConfig()
		}

		// Allow override via env
		if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
			var level zapcore.Level
			if err := level.UnmarshalText([]byte(lvl)); err == nil {
				cfg.Level = zap.NewAtomicLevelAt(level)
			}
		}

		l, err := cfg.Build(zap.AddCallerSkip(0))
		if err != nil {
			// Fallback to nop if build fails (should never happen)
			l = zap.NewNop()
		}
		global = l
	})
}

// L returns the global logger. If Init hasn't been called, returns a no-op logger.
func L() *zap.Logger {
	if global == nil {
		return zap.NewNop()
	}
	return global
}

// S returns the global sugared logger for printf-style logging.
func S() *zap.SugaredLogger {
	return L().Sugar()
}

// WithCtx returns a new context with the given logger attached.
func WithCtx(ctx context.Context, l *zap.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromCtx extracts the logger from context, or returns the global logger.
func FromCtx(ctx context.Context) *zap.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*zap.Logger); ok {
		return l
	}
	return L()
}

// Sync flushes any buffered log entries. Call before application exit.
func Sync() {
	if global != nil {
		_ = global.Sync()
	}
}
