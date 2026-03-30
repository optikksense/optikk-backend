package servicepage

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetTotalServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetTotalServices(ctx, teamID, startMs, endMs)
}

func (s *Service) GetHealthyServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetHealthyServices(ctx, teamID, startMs, endMs)
}

func (s *Service) GetDegradedServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetDegradedServices(ctx, teamID, startMs, endMs)
}

func (s *Service) GetUnhealthyServices(ctx context.Context, teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetUnhealthyServices(ctx, teamID, startMs, endMs)
}

func (s *Service) GetServiceMetrics(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceMetric, error) {
	return s.repo.GetServiceMetrics(ctx, teamID, startMs, endMs)
}

func (s *Service) GetServiceTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeSeriesPoint, error) {
	return s.repo.GetServiceTimeSeries(ctx, teamID, startMs, endMs)
}

func (s *Service) GetServiceEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	return s.repo.GetServiceEndpoints(ctx, teamID, startMs, endMs, serviceName)
}

func (s *Service) GetNavigator(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceHealth, error) {
	rows, err := s.repo.GetServiceHealth(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	result := make([]ServiceHealth, len(rows))
	for i, r := range rows {
		result[i] = ServiceHealth{
			ServiceName:  r.ServiceName,
			RequestCount: r.RequestCount,
			ErrorCount:   r.ErrorCount,
			ErrorRate:    r.ErrorRate,
			P95LatencyMs: r.P95LatencyMs,
			HealthStatus: deriveHealthStatus(r.ErrorRate),
		}
	}
	return result, nil
}

func deriveHealthStatus(errorRate float64) string {
	switch {
	case errorRate <= HealthyMaxErrorRate:
		return "healthy"
	case errorRate <= DegradedMaxErrorRate:
		return "degraded"
	default:
		return "critical"
	}
}
