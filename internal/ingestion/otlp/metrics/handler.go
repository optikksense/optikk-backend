package metrics

import (
	"log/slog"
	"context"

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
	resp, err := h.service.Export(ctx, req)
	if err == nil {
		slog.Info("ingest: processed metrics via gRPC")
	}
	return resp, err
}
