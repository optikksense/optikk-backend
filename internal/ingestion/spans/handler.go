// Package spans is the OTLP spans ingestion path: gRPC handler → mapper →
// producer (Kafka) → consumer (Kafka) → writer (ClickHouse). One file per
// stage, no in-memory accumulation. Producer-side batching is delegated to
// franz-go (linger / batch_max_bytes); consumer flushes whatever a single
// PollFetches returns to CH as one batch insert; CH async_insert=1 provides
// server-side coalescing on top.
package spans

import (
	"context"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler is the gRPC TraceServiceServer. Auth is enforced upstream by the
// auth interceptor; teamID is read from ctx.
type Handler struct {
	tracepb.UnimplementedTraceServiceServer
	producer *Producer
}

func NewHandler(p *Producer) *Handler { return &Handler{producer: p} }

func (h *Handler) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapRequest(teamID, req)
	metrics.MapperDuration.WithLabelValues("spans").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("spans").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}
	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("spans", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "spans handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("spans", "ok").Observe(time.Since(pubStart).Seconds())
	return &tracepb.ExportTraceServiceResponse{}, nil
}
