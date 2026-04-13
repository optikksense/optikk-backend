package metrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	auth       ingestion.TeamResolver
	dispatcher ingestion.Dispatcher[*MetricRow]
	tracker    ingestion.SizeTracker
}

func NewService(authenticator ingestion.TeamResolver, d ingestion.Dispatcher[*MetricRow], tracker ingestion.SizeTracker) *Service {
	return &Service{
		auth:       authenticator,
		dispatcher: d,
		tracker:    tracker,
	}
}

func (s *Service) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	teamID, err := otlp.ResolveTeamID(ctx, s.auth)
	if err != nil {
		return nil, err
	}

	rows := mapMetrics(teamID, req)
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}
	if err := s.dispatcher.Dispatch(ingestion.TelemetryBatch[*MetricRow]{
		TeamID: teamID,
		Rows:   rows,
	}); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &metricpb.ExportMetricsServiceResponse{}, nil
}
