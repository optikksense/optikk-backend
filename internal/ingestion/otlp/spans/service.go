package spans

import (
	"context"
	"errors"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
	serviceinventory "github.com/Optikk-Org/optikk-backend/internal/modules/services/inventory"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	auth    otlp.TeamResolver
	queue   otlp.Queue
	tracker otlp.SizeTracker
	limiter otlp.Limiter
	projector *serviceinventory.Projector
}

func NewService(authenticator otlp.TeamResolver, queue otlp.Queue, tracker otlp.SizeTracker, limiter otlp.Limiter, projector *serviceinventory.Projector) *Service {
	return &Service{
		auth:      authenticator,
		queue:     queue,
		tracker:   tracker,
		limiter:   limiter,
		projector: projector,
	}
}

func (s *Service) Export(ctx context.Context, req *tracepb.ExportTraceServiceRequest) (*tracepb.ExportTraceServiceResponse, error) {
	teamID, err := otlp.ResolveTeamID(ctx, s.auth)
	if err != nil {
		return nil, err
	}

	rows := mapSpans(teamID, req)
	if len(rows) == 0 {
		return &tracepb.ExportTraceServiceResponse{}, nil
	}
	if !s.limiter.Allow(teamID, int64(len(rows))) {
		return nil, status.Error(codes.ResourceExhausted, "team ingest rate limit exceeded")
	}
	if err := s.queue.Enqueue(rows); err != nil {
		if errors.Is(err, ingest.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, "ingest queue full")
		}
		return nil, status.Errorf(codes.Internal, "failed to enqueue spans: %v", err)
	}
	if s.projector != nil {
		if err := s.projector.ObserveTraceExport(teamID, req); err != nil {
			logger.L().Warn("service inventory observe failed", "team_id", teamID, "error", err)
		}
	}
	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &tracepb.ExportTraceServiceResponse{}, nil
}
