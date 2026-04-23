package spans

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"log/slog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler is the gRPC TraceServiceServer. It does no auth itself: the team
// id was resolved by auth.UnaryInterceptor and is read from ctx.
type Handler struct {
	tracepb.UnimplementedTraceServiceServer
	producer *Producer
}

func NewHandler(p *Producer) *Handler {
	return &Handler{producer: p}
}

func (h *Handler) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	rows := MapRequest(teamID, req)
	slog.Info("spans handler: received request", slog.Int("rows", len(rows)))
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}
	if err := h.producer.Publish(ctx, rows); err != nil {
		slog.Error("spans handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &tracepb.ExportTraceServiceResponse{}, nil
}
