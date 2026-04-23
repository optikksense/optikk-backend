// Package otel bootstraps the OpenTelemetry SDK for self-telemetry.
// Traces flow to the separate LGTM monitoring stack's otel-collector
// (see docker-compose.yml:otel-collector @ host-port 14317), NOT to the
// app's own OTLP receiver on 4317 — the two stacks stay isolated so a
// customer ingest outage does not break our own observability.
package otel

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Init wires the global tracer provider + W3C propagator. Returns a
// shutdown closer wired into the app's graceful-shutdown chain. When
// disabled in config it installs a no-op provider so `otel.Tracer(...)`
// calls scattered through the codebase are safe.
func Init(ctx context.Context, cfg config.OTelConfig, env string) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: otlp exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
		sdktrace.WithResource(newResource(cfg.ServiceName, env)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
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
