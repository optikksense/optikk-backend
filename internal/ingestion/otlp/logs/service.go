package logs

import (
	"context"
	"errors"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	auth    otlp.TeamResolver
	queue   otlp.Queue
	tracker otlp.SizeTracker
	limiter otlp.Limiter
}

func NewService(authenticator otlp.TeamResolver, queue otlp.Queue, tracker otlp.SizeTracker, limiter otlp.Limiter) *Service {
	return &Service{
		auth:    authenticator,
		queue:   queue,
		tracker: tracker,
		limiter: limiter,
	}
}

func (s *Service) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, err := otlp.ResolveTeamID(ctx, s.auth)
	if err != nil {
		return nil, err
	}

	rows := mapLogs(teamID, req)
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}
	if !s.limiter.Allow(teamID, int64(len(rows))) {
		return nil, status.Error(codes.ResourceExhausted, "team ingest rate limit exceeded")
	}
	if err := s.queue.Enqueue(rows); err != nil {
		if errors.Is(err, ingest.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
		}
		return nil, status.Errorf(codes.Internal, "failed to enqueue logs: %v", err)
	}
	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &logspb.ExportLogsServiceResponse{}, nil
}
