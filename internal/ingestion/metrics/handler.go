package metrics

import (
	"context"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	obsmetrics "github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
		obsmetrics.IngestRecordsTotal.WithLabelValues("metrics", "err").Add(float64(len(rows)))
		slog.ErrorContext(ctx, "metrics handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	obsmetrics.IngestRecordsTotal.WithLabelValues("metrics", "ok").Add(float64(len(rows)))
	if size := proto.Size(req); size > 0 {
		obsmetrics.IngestRecordBytes.WithLabelValues("metrics").Add(float64(size))
	}
	return &metricspb.ExportMetricsServiceResponse{}, nil
}
