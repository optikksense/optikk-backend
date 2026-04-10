package metrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/infra/ingestion"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
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
	// Dispatch to internal channels (persistence + streaming)
	s.dispatcher.Dispatch(ingestion.TelemetryBatch[*MetricRow]{
		TeamID: teamID,
		Rows:   rows,
	})

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &metricpb.ExportMetricsServiceResponse{}, nil
}
