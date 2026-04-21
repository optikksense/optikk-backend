package logs

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler is the gRPC LogsServiceServer. It does no auth itself: the team id
// was resolved by auth.UnaryInterceptor and is read from ctx. The only work
// the handler does on the hot path is map OTLP → Row → Produce. Persistence
// happen out-of-band in consumer.go.
type Handler struct {
	logspb.UnimplementedLogsServiceServer
	producer *Producer
}

func NewHandler(p *Producer) *Handler {
	return &Handler{producer: p}
}

func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	rows := MapRequest(teamID, req)
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}
	if err := h.producer.Publish(ctx, rows); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &logspb.ExportLogsServiceResponse{}, nil
}
