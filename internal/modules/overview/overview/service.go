package overview

import ()

// Service encapsulates the business logic for the overview module.
type Service interface {
	GetSummary(teamUUID string, startMs, endMs int64) (Summary, error)
	GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error)
	GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
	GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
}

// OverviewService provides business logic orchestration for overview dashboards.
type OverviewService struct {
	repo Repository
}

// NewService creates a new OverviewService.
func NewService(repo Repository) Service {
	return &OverviewService{repo: repo}
}

func (s *OverviewService) GetSummary(teamUUID string, startMs, endMs int64) (Summary, error) {
	return s.repo.GetSummary(teamUUID, startMs, endMs)
}

func (s *OverviewService) GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetTimeSeries(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error) {
	return s.repo.GetServices(teamUUID, startMs, endMs)
}

func (s *OverviewService) GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	return s.repo.GetEndpointMetrics(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetEndpointTimeSeries(teamUUID, startMs, endMs, serviceName)
}
