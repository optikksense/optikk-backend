package metrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/auth"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the OTLP gRPC metrics ingestion endpoint.
type Service struct {
	metricspb.UnimplementedMetricsServiceServer
	auth       ingestion.TeamResolver
	dispatcher *kafka.Dispatcher[*proto.MetricRow]
	tracker    ingestion.SizeTracker
}

func NewService(authenticator ingestion.TeamResolver, d *kafka.Dispatcher[*proto.MetricRow], tracker ingestion.SizeTracker) *Service {
	return &Service{
		auth:       authenticator,
		dispatcher: d,
		tracker:    tracker,
	}
}

func (s *Service) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	teamID, err := auth.ResolveFromContext(ctx, s.auth)
	if err != nil {
		return nil, err
	}

	rows := mapMetrics(teamID, req)
	if len(rows) == 0 {
		return &metricspb.ExportMetricsServiceResponse{}, nil
	}

	for _, row := range rows {
		if err := s.dispatcher.Dispatch(ctx, row); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
	}

	auth.TrackPayloadSize(s.tracker, teamID, req)
	return &metricspb.ExportMetricsServiceResponse{}, nil
}
