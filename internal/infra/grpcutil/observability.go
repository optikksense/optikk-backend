// Package grpcutil hosts cross-cutting gRPC helpers. `observability.go`
// exposes the unary + stream server interceptors that record Prometheus
// timing and emit per-RPC structured slog lines. Chain order: our
// interceptors run inside otelgrpc's stats handler so the context they
// see already carries the server-side trace span.
package grpcutil

import (
	"context"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryObservability wraps unary handlers. Records `grpc_started_total`,
// `grpc_handling_duration_seconds`, `grpc_handled_total`, and logs one
// `grpc request …` line per completed RPC.
func UnaryObservability() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		metrics.GRPCStarted.WithLabelValues(info.FullMethod).Inc()
		start := time.Now()
		resp, err := handler(ctx, req)
		recordGRPCResult(ctx, info.FullMethod, start, err)
		return resp, err
	}
}

// StreamObservability wraps streaming handlers. Same envelope as the
// unary flavour — timing is from handler invocation to return.
func StreamObservability() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		metrics.GRPCStarted.WithLabelValues(info.FullMethod).Inc()
		start := time.Now()
		err := handler(srv, ss)
		recordGRPCResult(ss.Context(), info.FullMethod, start, err)
		return err
	}
}

func recordGRPCResult(ctx context.Context, fullMethod string, start time.Time, err error) {
	dur := time.Since(start)
	code := status.Code(err).String()
	metrics.GRPCDuration.WithLabelValues(fullMethod).Observe(dur.Seconds())
	metrics.GRPCHandled.WithLabelValues(fullMethod, code).Inc()

	sc := trace.SpanContextFromContext(ctx)
	attrs := []any{
		slog.String("method", fullMethod),
		slog.String("code", code),
		slog.Float64("duration_ms", float64(dur.Nanoseconds())/1e6),
	}
	if sc.IsValid() {
		attrs = append(attrs,
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
		slog.WarnContext(ctx, "grpc request failed", attrs...)
		return
	}
	slog.InfoContext(ctx, "grpc request", attrs...)
}
