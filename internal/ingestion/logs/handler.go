package logs

import (
	"context"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
		metrics.IngestRecordsTotal.WithLabelValues("logs", "err").Add(float64(len(rows)))
		slog.ErrorContext(ctx, "logs handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.IngestRecordsTotal.WithLabelValues("logs", "ok").Add(float64(len(rows)))
	if size := proto.Size(req); size > 0 {
		metrics.IngestRecordBytes.WithLabelValues("logs").Add(float64(size))
	}
	return &logspb.ExportLogsServiceResponse{}, nil
}
