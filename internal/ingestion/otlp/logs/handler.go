package logs

import (
	"log/slog"
	"context"

	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

type Handler struct {
	logspb.UnimplementedLogsServiceServer
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	resp, err := h.service.Export(ctx, req)
	if err == nil {
		slog.Info("ingest: processed logs via gRPC")
	}
	return resp, err
}
