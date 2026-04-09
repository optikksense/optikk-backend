package overview

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error) {
	rows, err := s.repo.GetRequestRate(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapRequestRateRows(rows), nil
}

func (s *Service) GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error) {
	rows, err := s.repo.GetErrorRate(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapErrorRateRows(rows), nil
}

func (s *Service) GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error) {
	rows, err := s.repo.GetP95Latency(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapP95LatencyRows(rows), nil
}

func (s *Service) GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceMetric, error) {
	rows, err := s.repo.GetServices(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapServiceMetricRows(rows), nil
}

func (s *Service) GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	rows, err := s.repo.GetTopEndpoints(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapEndpointMetricRows(rows), nil
}

func (s *Service) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (GlobalSummary, error) {
	row, err := s.repo.GetSummary(ctx, teamID, startMs, endMs)
	if err != nil {
		return GlobalSummary{}, err
	}
	return mapGlobalSummaryRow(row), nil
}
