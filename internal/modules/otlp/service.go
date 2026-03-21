package otlp

import (
	"context"
	"errors"

	"github.com/observability/observability-backend-go/internal/modules/otlp/auth"
	"github.com/observability/observability-backend-go/internal/modules/otlp/internal/ingest"
	"github.com/observability/observability-backend-go/internal/modules/otlp/internal/mapper"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	auth         *auth.Authenticator
	spansQueue   *ingest.Queue
	logsQueue    *ingest.Queue
	metricsQueue *ingest.Queue
	tracker      *ingest.ByteTracker
	limiter      *ingest.TeamLimiter
}

func NewService(
	authenticator *auth.Authenticator,
	spansQueue *ingest.Queue,
	logsQueue *ingest.Queue,
	metricsQueue *ingest.Queue,
	tracker *ingest.ByteTracker,
	limiter *ingest.TeamLimiter,
) *Service {
	return &Service{
		auth:         authenticator,
		spansQueue:   spansQueue,
		logsQueue:    logsQueue,
		metricsQueue: metricsQueue,
		tracker:      tracker,
		limiter:      limiter,
	}
}

func (s *Service) trackIngestSize(teamID int64, msg proto.Message) {
	if s.tracker == nil || teamID <= 0 || msg == nil {
		return
	}
	size := proto.Size(msg)
	if size <= 0 {
		return
	}
	s.tracker.Track(teamID, int64(size))
}

func (s *Service) resolveTeamID(ctx context.Context) (int64, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, status.Error(codes.Unauthenticated, "missing metadata")
	}

	keys := md.Get("x-api-key")
	if len(keys) == 0 {
		return 0, status.Error(codes.Unauthenticated, "missing x-api-key metadata header")
	}

	teamID, err := s.auth.ResolveTeamID(ctx, keys[0])
	if err != nil {
		if errors.Is(err, auth.ErrMissingAPIKey) || errors.Is(err, auth.ErrInvalidAPIKey) {
			return 0, status.Error(codes.Unauthenticated, err.Error())
		}
		return 0, status.Error(codes.Internal, err.Error())
	}
	return teamID, nil
}

func (s *Service) ExportTraces(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	teamID, err := s.resolveTeamID(ctx)
	if err != nil {
		return nil, err
	}

	rows := mapper.MapSpans(teamID, req)
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}
	if !s.limiter.Allow(teamID, int64(len(rows))) {
		return nil, status.Error(codes.ResourceExhausted, "team ingest rate limit exceeded")
	}
	if err := s.spansQueue.Enqueue(rows); err != nil {
		if errors.Is(err, ingest.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
		}
		return nil, status.Errorf(codes.Internal, "failed to enqueue spans: %v", err)
	}
	s.trackIngestSize(teamID, req)
	return &tracepb.ExportTraceServiceResponse{}, nil
}

func (s *Service) ExportLogs(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, err := s.resolveTeamID(ctx)
	if err != nil {
		return nil, err
	}

	rows := mapper.MapLogs(teamID, req)
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}
	if !s.limiter.Allow(teamID, int64(len(rows))) {
		return nil, status.Error(codes.ResourceExhausted, "team ingest rate limit exceeded")
	}
	if err := s.logsQueue.Enqueue(rows); err != nil {
		if errors.Is(err, ingest.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
		}
		return nil, status.Errorf(codes.Internal, "failed to enqueue logs: %v", err)
	}
	s.trackIngestSize(teamID, req)
	return &logspb.ExportLogsServiceResponse{}, nil
}

func (s *Service) ExportMetrics(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	teamID, err := s.resolveTeamID(ctx)
	if err != nil {
		return nil, err
	}

	rows := mapper.MapMetrics(teamID, req)
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}
	if !s.limiter.Allow(teamID, int64(len(rows))) {
		return nil, status.Error(codes.ResourceExhausted, "team ingest rate limit exceeded")
	}
	if err := s.metricsQueue.Enqueue(rows); err != nil {
		if errors.Is(err, ingest.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
		}
		return nil, status.Errorf(codes.Internal, "failed to enqueue metrics: %v", err)
	}
	s.trackIngestSize(teamID, req)
	return &metricspb.ExportMetricsServiceResponse{}, nil
}
