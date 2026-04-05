package logs

import (
	"context"
	"log/slog"

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
	return h.service.Export(ctx, req)
}
