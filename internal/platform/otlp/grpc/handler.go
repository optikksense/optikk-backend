package grpc

import (
	"context"
	"errors"

	"github.com/observability/observability-backend-go/internal/platform/ingest"
	"github.com/observability/observability-backend-go/internal/platform/otlp/auth"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Handler implements the three OTel gRPC collector services.
type Handler struct {
	auth           *auth.Authenticator
	resourcesQueue *ingest.Queue
	spansQueue     *ingest.Queue
	logsQueue      *ingest.Queue
	metricsQueue   *ingest.Queue

	TraceServer   *TraceServer
	LogsServer    *LogsServer
	MetricsServer *MetricsServer
}

type TraceServer struct {
	tracepb.UnimplementedTraceServiceServer
	h *Handler
}

type LogsServer struct {
	logspb.UnimplementedLogsServiceServer
	h *Handler
}

type MetricsServer struct {
	metricspb.UnimplementedMetricsServiceServer
	h *Handler
}

// NewHandler creates a new gRPC OTLP receiver handler.
func NewHandler(
	auth *auth.Authenticator,
	resourcesQueue *ingest.Queue,
	spansQueue *ingest.Queue,
	logsQueue *ingest.Queue,
	metricsQueue *ingest.Queue,
) *Handler {
	h := &Handler{
		auth:           auth,
		resourcesQueue: resourcesQueue,
		spansQueue:     spansQueue,
		logsQueue:      logsQueue,
		metricsQueue:   metricsQueue,
	}
	h.TraceServer = &TraceServer{h: h}
	h.LogsServer = &LogsServer{h: h}
	h.MetricsServer = &MetricsServer{h: h}
	return h
}

// resolveTeamID extracts "x-api-key" from gRPC metadata and validates against the Authenticator.
func (h *Handler) resolveTeamID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	keys := md.Get("x-api-key")
	if len(keys) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing x-api-key metadata header")
	}
	apiKey := keys[0]

	teamID, err := h.auth.ResolveTeamID(ctx, apiKey)
	if err != nil {
		if errors.Is(err, auth.ErrMissingAPIKey) || errors.Is(err, auth.ErrInvalidAPIKey) {
			return "", status.Error(codes.Unauthenticated, err.Error())
		}
		return "", status.Error(codes.Internal, err.Error())
	}

	return teamID, nil
}

// Export trace payloads.
func (s *TraceServer) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	teamID, err := s.h.resolveTeamID(ctx)
	if err != nil {
		return nil, err
	}

	result := MapTraceRows(teamID, req)
	if len(result.Resources) == 0 && len(result.Spans) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}

	if len(result.Resources) > 0 {
		if err := s.h.resourcesQueue.Enqueue(result.Resources); err != nil {
			if errors.Is(err, ingest.ErrBackpressure) {
				return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
			}
			return nil, status.Errorf(codes.Internal, "failed to enqueue resources: %v", err)
		}
	}

	if len(result.Spans) > 0 {
		if err := s.h.spansQueue.Enqueue(result.Spans); err != nil {
			if errors.Is(err, ingest.ErrBackpressure) {
				return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
			}
			return nil, status.Errorf(codes.Internal, "failed to enqueue spans: %v", err)
		}
	}

	return &tracepb.ExportTraceServiceResponse{}, nil
}

// Export logs payloads.
func (s *LogsServer) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, err := s.h.resolveTeamID(ctx)
	if err != nil {
		return nil, err
	}

	rows := MapLogs(teamID, req)
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}

	if err := s.h.logsQueue.Enqueue(rows); err != nil {
		if errors.Is(err, ingest.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
		}
		return nil, status.Errorf(codes.Internal, "failed to enqueue logs: %v", err)
	}

	return &logspb.ExportLogsServiceResponse{}, nil
}

// Export metrics payloads.
func (s *MetricsServer) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	teamID, err := s.h.resolveTeamID(ctx)
	if err != nil {
		return nil, err
	}

	rows := MapMetrics(teamID, req)
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}

	if err := s.h.metricsQueue.Enqueue(rows); err != nil {
		if errors.Is(err, ingest.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
		}
		return nil, status.Errorf(codes.Internal, "failed to enqueue metrics: %v", err)
	}

	return &metricspb.ExportMetricsServiceResponse{}, nil
}
