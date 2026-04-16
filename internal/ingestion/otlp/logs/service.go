package logs

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/kafkadispatcher"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	auth       ingestion.TeamResolver
	dispatcher *kafkadispatcher.KafkaDispatcher[*LogRow]
	tracker    ingestion.SizeTracker
}

func NewService(authenticator ingestion.TeamResolver, d *kafkadispatcher.KafkaDispatcher[*LogRow], tracker ingestion.SizeTracker) *Service {
	return &Service{
		auth:       authenticator,
		dispatcher: d,
		tracker:    tracker,
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
	if err := s.dispatcher.Dispatch(ingestion.TelemetryBatch[*LogRow]{
		TeamID: teamID,
		Rows:   rows,
	}); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &logspb.ExportLogsServiceResponse{}, nil
}
