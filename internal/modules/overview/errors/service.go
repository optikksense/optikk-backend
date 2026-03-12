package errors

import ()

type Service interface {
	GetServiceErrorRate(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorVolume(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetLatencyDuringErrorWindows(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorGroups(teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error)
}

type ErrorService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &ErrorService{repo: repo}
}

func (s *ErrorService) GetErrorGroups(teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	return s.repo.GetErrorGroups(teamID, startMs, endMs, serviceName, limit)
}

func (s *ErrorService) GetServiceErrorRate(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetServiceErrorRate(teamID, startMs, endMs, serviceName)
}

func (s *ErrorService) GetErrorVolume(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetErrorVolume(teamID, startMs, endMs, serviceName)
}

func (s *ErrorService) GetLatencyDuringErrorWindows(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetLatencyDuringErrorWindows(teamID, startMs, endMs, serviceName)
}
