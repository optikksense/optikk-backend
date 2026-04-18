package metrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler is the gRPC MetricsServiceServer. It does no auth itself: the team
// id was resolved by auth.UnaryInterceptor and is read from ctx.
type Handler struct {
	metricspb.UnimplementedMetricsServiceServer
	producer *Producer
}

func NewHandler(p *Producer) *Handler {
	return &Handler{producer: p}
}

func (h *Handler) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	rows := MapRequest(teamID, req)
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}
	if err := h.producer.Publish(ctx, rows); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &metricspb.ExportMetricsServiceResponse{}, nil
}
