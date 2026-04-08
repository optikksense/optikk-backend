package metrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type Service struct {
	auth       platformingestion.TeamResolver
	dispatcher platformingestion.Dispatcher[*MetricRow]
	tracker    platformingestion.SizeTracker
	limiter    platformingestion.Limiter
}

func NewService(authenticator platformingestion.TeamResolver, d platformingestion.Dispatcher[*MetricRow], tracker platformingestion.SizeTracker, limiter platformingestion.Limiter) *Service {
	return &Service{
		auth:       authenticator,
		dispatcher: d,
		tracker:    tracker,
		limiter:    limiter,
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
	s.dispatcher.Dispatch(platformingestion.TelemetryBatch[*MetricRow]{
		TeamID: teamID,
		Rows:   rows,
	})

	otlp.TrackPayloadSize(s.tracker, teamID, req)
	return &metricpb.ExportMetricsServiceResponse{}, nil
}
