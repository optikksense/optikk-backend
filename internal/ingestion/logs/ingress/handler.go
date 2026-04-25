package ingress

import (
	"context"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/auth"
	"github.com/Optikk-Org/optikk-backend/internal/infra/metrics"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/mapper"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Handler is the gRPC LogsServiceServer. It does no auth itself: the team id
// was resolved by auth.UnaryInterceptor and is read from ctx. The only work
// the handler does on the hot path is map OTLP → Row → Produce. Persistence
// runs out-of-band in the consumer-side dispatcher/worker pipeline.
type Handler struct {
	logspb.UnimplementedLogsServiceServer
	producer *Producer
}

func NewHandler(p *Producer) *Handler {
	return &Handler{producer: p}
}

// Export records per-stage latency so Grafana can attribute p99 spikes to
// mapper vs. Kafka publish rather than aggregating them into one black box.
func (h *Handler) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, ok := auth.TeamIDFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "team id missing from context")
	}
	mapStart := time.Now()
	rows := mapper.MapRequest(teamID, req)
	metrics.MapperDuration.WithLabelValues("logs").Observe(time.Since(mapStart).Seconds())
	metrics.MapperRowsPerRequest.WithLabelValues("logs").Observe(float64(len(rows)))
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}
	pubStart := time.Now()
	err := h.producer.Publish(ctx, rows)
	elapsed := time.Since(pubStart).Seconds()
	if err != nil {
		metrics.HandlerPublishDuration.WithLabelValues("logs", "err").Observe(elapsed)
		metrics.IngestRecordsTotal.WithLabelValues("logs", "err").Add(float64(len(rows)))
		slog.ErrorContext(ctx, "logs handler: publish failed", slog.Any("error", err))
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	metrics.HandlerPublishDuration.WithLabelValues("logs", "ok").Observe(elapsed)
	metrics.IngestRecordsTotal.WithLabelValues("logs", "ok").Add(float64(len(rows)))
	if size := proto.Size(req); size > 0 {
		metrics.IngestRecordBytes.WithLabelValues("logs").Add(float64(size))
	}
	return &logspb.ExportLogsServiceResponse{}, nil
}
