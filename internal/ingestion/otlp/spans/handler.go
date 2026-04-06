package spans

import (
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
	return h.service.Export(ctx, req)
}
