package servicepage

import ()

// Service encapsulates the business logic for the services overview module.
type Service interface {
	GetTotalServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetHealthyServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetDegradedServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetUnhealthyServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetServiceMetrics(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error)
	GetServiceTimeSeries(teamUUID string, startMs, endMs int64) ([]TimeSeriesPoint, error)
	GetServiceEndpoints(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
}

// ServicePageService provides business logic orchestration for services overview.
type ServicePageService struct {
	repo Repository
}

// NewService creates a new services overview service.
func NewService(repo Repository) Service {
	return &ServicePageService{repo: repo}
}

func (s *ServicePageService) GetTotalServices(teamUUID string, startMs, endMs int64) (int64, error) {
	return s.repo.GetTotalServices(teamUUID, startMs, endMs)
}

func (s *ServicePageService) GetHealthyServices(teamUUID string, startMs, endMs int64) (int64, error) {
	return s.repo.GetHealthyServices(teamUUID, startMs, endMs)
}

func (s *ServicePageService) GetDegradedServices(teamUUID string, startMs, endMs int64) (int64, error) {
	return s.repo.GetDegradedServices(teamUUID, startMs, endMs)
}

func (s *ServicePageService) GetUnhealthyServices(teamUUID string, startMs, endMs int64) (int64, error) {
	return s.repo.GetUnhealthyServices(teamUUID, startMs, endMs)
}

func (s *ServicePageService) GetServiceMetrics(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error) {
	return s.repo.GetServiceMetrics(teamUUID, startMs, endMs)
}

func (s *ServicePageService) GetServiceTimeSeries(teamUUID string, startMs, endMs int64) ([]TimeSeriesPoint, error) {
	return s.repo.GetServiceTimeSeries(teamUUID, startMs, endMs)
}

func (s *ServicePageService) GetServiceEndpoints(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	return s.repo.GetServiceEndpoints(teamUUID, startMs, endMs, serviceName)
}
