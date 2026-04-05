package metrics

import (
	"context"
	"log/slog"

	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type Handler struct {
	metricspb.UnimplementedMetricsServiceServer
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	return h.service.Export(ctx, req)
}
