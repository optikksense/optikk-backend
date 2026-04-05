package spans

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

type Service struct {
	auth       otlp.TeamResolver
	dispatcher *otlp.Dispatcher
	tracker    otlp.SizeTracker
	limiter    otlp.Limiter
}

func NewService(authenticator otlp.TeamResolver, d *otlp.Dispatcher, tracker otlp.SizeTracker, limiter otlp.Limiter) *Service {
	return &Service{
		auth:       authenticator,
		dispatcher: d,
		tracker:    tracker,
		limiter:    limiter,
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
	s.dispatcher.Dispatch(otlp.TelemetryBatch{
		Signal: otlp.SignalSpan,
		TeamID: teamID,
		Rows:   rows,
	})

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &tracepb.ExportTraceServiceResponse{}, nil
}
