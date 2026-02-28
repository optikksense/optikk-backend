package service

import (
	"github.com/observability/observability-backend-go/internal/modules/overview/errors/model"
	"github.com/observability/observability-backend-go/internal/modules/overview/errors/store"
)

// Service encapsulates the business logic for the overview errors module.
type Service interface {
	GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error)
}

// ErrorService provides business logic orchestration for overview error dashboards.
type ErrorService struct {
	repo store.Repository
}

// NewService creates a new ErrorService.
func NewService(repo store.Repository) Service {
	return &ErrorService{repo: repo}
}

func (s *ErrorService) GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error) {
	return s.repo.GetErrorGroups(teamUUID, startMs, endMs, serviceName, limit)
}

func (s *ErrorService) GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetServiceErrorRate(teamUUID, startMs, endMs, serviceName)
}

func (s *ErrorService) GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetErrorVolume(teamUUID, startMs, endMs, serviceName)
}

func (s *ErrorService) GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetLatencyDuringErrorWindows(teamUUID, startMs, endMs, serviceName)
}
