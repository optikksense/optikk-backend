package otlp

import (
	"context"

	"github.com/observability/observability-backend-go/internal/platform/logger"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

type Handler struct {
	TraceServer   *TraceServer
	LogsServer    *LogsServer
	MetricsServer *MetricsServer
}

type TraceServer struct {
	tracepb.UnimplementedTraceServiceServer
	service *Service
}

type LogsServer struct {
	logspb.UnimplementedLogsServiceServer
	service *Service
}

type MetricsServer struct {
	metricspb.UnimplementedMetricsServiceServer
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{
		TraceServer:   &TraceServer{service: service},
		LogsServer:    &LogsServer{service: service},
		MetricsServer: &MetricsServer{service: service},
	}
}

func (s *TraceServer) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	resp, err := s.service.ExportTraces(ctx, req)
	if err == nil {
		logger.L().Info("ingest: processed traces via gRPC")
	}
	return resp, err
}

func (s *LogsServer) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	resp, err := s.service.ExportLogs(ctx, req)
	if err == nil {
		logger.L().Info("ingest: processed logs via gRPC")
	}
	return resp, err
}

func (s *MetricsServer) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	resp, err := s.service.ExportMetrics(ctx, req)
	if err == nil {
		logger.L().Info("ingest: processed metrics via gRPC")
	}
	return resp, err
}
