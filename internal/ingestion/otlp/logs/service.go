package logs

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
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

func (s *Service) Export(ctx context.Context, req *logspb.ExportLogsServiceRequest) (*logspb.ExportLogsServiceResponse, error) {
	teamID, err := otlp.ResolveTeamID(ctx, s.auth)
	if err != nil {
		return nil, err
	}

	rows := mapLogs(teamID, req)
	if len(rows) == 0 {
		return &logspb.ExportLogsServiceResponse{}, nil
	}
	// Dispatch to internal channels (persistence + streaming)
	s.dispatcher.Dispatch(otlp.TelemetryBatch{
		Signal: otlp.SignalLog,
		TeamID: teamID,
		Rows:   rows,
	})

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &logspb.ExportLogsServiceResponse{}, nil
}
