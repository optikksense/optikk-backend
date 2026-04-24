package spans

import (
	"context"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	slog.InfoContext(ctx, "spans handler: received request", slog.Int("rows", len(rows)))
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}
	if err := h.producer.Publish(ctx, rows); err != nil {
		metrics.IngestRecordsTotal.WithLabelValues("spans", "err").Add(float64(len(rows)))
		slog.ErrorContext(ctx, "spans handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.IngestRecordsTotal.WithLabelValues("spans", "ok").Add(float64(len(rows)))
	if size := proto.Size(req); size > 0 {
		metrics.IngestRecordBytes.WithLabelValues("spans").Add(float64(size))
	}
	return &tracepb.ExportTraceServiceResponse{}, nil
}
