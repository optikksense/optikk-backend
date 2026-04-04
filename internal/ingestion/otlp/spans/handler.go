package spans

import (
	"log/slog"
	"context"

	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

type Handler struct {
	tracepb.UnimplementedTraceServiceServer
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	resp, err := h.service.Export(ctx, req)
	if err == nil {
		slog.Info("ingest: processed traces via gRPC")
	}
	return resp, err
}
