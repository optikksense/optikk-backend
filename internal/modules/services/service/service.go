package servicepage

import ()

type Service interface {
	GetTotalServices(teamID int64, startMs, endMs int64) (int64, error)
	GetHealthyServices(teamID int64, startMs, endMs int64) (int64, error)
	GetDegradedServices(teamID int64, startMs, endMs int64) (int64, error)
	GetUnhealthyServices(teamID int64, startMs, endMs int64) (int64, error)
	GetServiceMetrics(teamID int64, startMs, endMs int64) ([]ServiceMetric, error)
	GetServiceTimeSeries(teamID int64, startMs, endMs int64) ([]TimeSeriesPoint, error)
	GetServiceEndpoints(teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
}

type ServicePageService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &ServicePageService{repo: repo}
}

func (s *ServicePageService) GetTotalServices(teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetTotalServices(teamID, startMs, endMs)
}

func (s *ServicePageService) GetHealthyServices(teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetHealthyServices(teamID, startMs, endMs)
}

func (s *ServicePageService) GetDegradedServices(teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetDegradedServices(teamID, startMs, endMs)
}

func (s *ServicePageService) GetUnhealthyServices(teamID int64, startMs, endMs int64) (int64, error) {
	return s.repo.GetUnhealthyServices(teamID, startMs, endMs)
}

func (s *ServicePageService) GetServiceMetrics(teamID int64, startMs, endMs int64) ([]ServiceMetric, error) {
	return s.repo.GetServiceMetrics(teamID, startMs, endMs)
}

func (s *ServicePageService) GetServiceTimeSeries(teamID int64, startMs, endMs int64) ([]TimeSeriesPoint, error) {
	return s.repo.GetServiceTimeSeries(teamID, startMs, endMs)
}

func (s *ServicePageService) GetServiceEndpoints(teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	return s.repo.GetServiceEndpoints(teamID, startMs, endMs, serviceName)
}
