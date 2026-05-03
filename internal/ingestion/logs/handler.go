// Package logs is the OTLP logs ingestion path. Same shape as ingestion/spans:
// handler → mapper → producer → consumer → writer, plus dlq + module.
package logs

import (
	"context"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	logspb.UnimplementedLogsServiceServer
	producer *Producer
}

func NewHandler(p *Producer) *Handler { return &Handler{producer: p} }

func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapRequest(teamID, req)
	metrics.MapperDuration.WithLabelValues("logs").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("logs").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}
	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("logs", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "logs handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("logs", "ok").Observe(time.Since(pubStart).Seconds())
	return &logspb.ExportLogsServiceResponse{}, nil
}
