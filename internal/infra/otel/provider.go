// Package otel bootstraps the OpenTelemetry SDK for self-telemetry.
// Traces + logs flow to the separate otel-collector (docker-compose
// `otel-collector` on host port 14317) which forwards to Grafana Cloud.
// We intentionally do NOT route through the app's own OTLP receiver on
// :4317 — the two stacks stay isolated so a customer ingest outage does
// not blind us to our own behaviour.
package otel

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Init wires the global tracer + logger providers and the W3C propagator.
// Returns a single shutdown closer that fans out to both providers so
// `app.Start` only waits once. When disabled in config it installs no-op
// providers so `otel.Tracer(...)` / `global.GetLoggerProvider()` callers
// scattered through the codebase are safe.
func Init(ctx context.Context, cfg config.OTelConfig, env string) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	res := newResource(cfg.ServiceName, env)

	tp, tErr := initTracerProvider(ctx, cfg, res)
	if tErr != nil {
		return nil, tErr
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	lp, lErr := initLoggerProvider(ctx, cfg, res)
	if lErr != nil {
		_ = tp.Shutdown(ctx)
		return nil, lErr
	}
	global.SetLoggerProvider(lp)

	return combinedShutdown(tp.Shutdown, lp.Shutdown), nil
}

// LoggerProviderFor is the accessor slogx.NewOtelBridgeHandler uses to
// obtain the current logger provider. Kept here so callers don't import
// go.opentelemetry.io/otel/log/global from random places.
func LoggerProviderFor() any { return global.GetLoggerProvider() }

func initTracerProvider(ctx context.Context, cfg config.OTelConfig, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: otlp trace exporter: %w", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		sdktrace.WithResource(res),
	), nil
}

func initLoggerProvider(ctx context.Context, cfg config.OTelConfig, res *resource.Resource) (*sdklog.LoggerProvider, error) {
	exp, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(cfg.Endpoint),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: otlp log exporter: %w", err)
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
		sdklog.WithResource(res),
	), nil
}

func combinedShutdown(fns ...func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		var firstErr error
		for _, fn := range fns {
			if err := fn(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}

func newResource(service, env string) *resource.Resource {
	name := service
	if name == "" {
		name = "optikk-backend"
	}
	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(name),
		semconv.DeploymentEnvironment(env),
	)
}
