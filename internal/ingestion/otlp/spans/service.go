package spans

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

type Service struct {
	auth       platformingestion.TeamResolver
	dispatcher platformingestion.Dispatcher[*SpanRow]
	tracker    platformingestion.SizeTracker
}

func NewService(authenticator platformingestion.TeamResolver, d platformingestion.Dispatcher[*SpanRow], tracker platformingestion.SizeTracker) *Service {
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
	// Dispatch to internal channels (persistence + streaming)
	s.dispatcher.Dispatch(platformingestion.TelemetryBatch[*SpanRow]{
		TeamID: teamID,
		Rows:   rows,
	})

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &tracepb.ExportTraceServiceResponse{}, nil
}
