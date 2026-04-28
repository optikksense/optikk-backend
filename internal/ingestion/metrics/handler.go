// Package metrics is the OTLP metrics ingestion path. Same shape as
// ingestion/spans + ingestion/logs.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	obsmetrics "github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	metricspb.UnimplementedMetricsServiceServer
	producer *Producer
}

func NewHandler(p *Producer) *Handler { return &Handler{producer: p} }

func (h *Handler) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapRequest(teamID, req)
	obsmetrics.MapperDuration.WithLabelValues("metrics").Observe(time.Since(mapStart).Seconds())
	obsmetrics.MapperRowsPerRequest.WithLabelValues("metrics").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}
	pubStart := time.Now()
	if err := h.producer.Publish(ctx, rows); err != nil {
		obsmetrics.HandlerPublishDuration.WithLabelValues("metrics", "err").Observe(time.Since(pubStart).Seconds())
		slog.ErrorContext(ctx, "metrics handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	obsmetrics.HandlerPublishDuration.WithLabelValues("metrics", "ok").Observe(time.Since(pubStart).Seconds())
	return &metricspb.ExportMetricsServiceResponse{}, nil
}
