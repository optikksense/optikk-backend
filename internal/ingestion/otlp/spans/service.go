package spans

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/kafkadispatcher"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	auth       ingestion.TeamResolver
	dispatcher *kafkadispatcher.KafkaDispatcher[*SpanRow]
	tracker    ingestion.SizeTracker
}

func NewService(authenticator ingestion.TeamResolver, d *kafkadispatcher.KafkaDispatcher[*SpanRow], tracker ingestion.SizeTracker) *Service {
	return &Service{
		auth:       authenticator,
		dispatcher: d,
		tracker:    tracker,
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
	if err := s.dispatcher.Dispatch(ingestion.TelemetryBatch[*SpanRow]{
		TeamID: teamID,
		Rows:   rows,
	}); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &tracepb.ExportTraceServiceResponse{}, nil
}
