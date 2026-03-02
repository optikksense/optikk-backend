package errors

import ()

// Service encapsulates the business logic for the overview errors module.
type Service interface {
	GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error)
}

// ErrorService provides business logic orchestration for overview error dashboards.
type ErrorService struct {
	repo Repository
}

// NewService creates a new ErrorService.
func NewService(repo Repository) Service {
	return &ErrorService{repo: repo}
}

func (s *ErrorService) GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	return s.repo.GetErrorGroups(teamUUID, startMs, endMs, serviceName, limit)
}

func (s *ErrorService) GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetServiceErrorRate(teamUUID, startMs, endMs, serviceName)
}

func (s *ErrorService) GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetErrorVolume(teamUUID, startMs, endMs, serviceName)
}

func (s *ErrorService) GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetLatencyDuringErrorWindows(teamUUID, startMs, endMs, serviceName)
}
