package service

import (
	"github.com/observability/observability-backend-go/internal/modules/services/service/model"
	"github.com/observability/observability-backend-go/internal/modules/services/service/store"
)

// Service encapsulates the business logic for the services overview module.
type Service interface {
	GetTotalServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetHealthyServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetDegradedServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetUnhealthyServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetServiceMetrics(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error)
	GetServiceTimeSeries(teamUUID string, startMs, endMs int64) ([]model.TimeSeriesPoint, error)
}

// ServicePageService provides business logic orchestration for services overview.
type ServicePageService struct {
	repo store.Repository
}

// NewService creates a new services overview service.
func NewService(repo store.Repository) Service {
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

func (s *ServicePageService) GetServiceMetrics(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error) {
	return s.repo.GetServiceMetrics(teamUUID, startMs, endMs)
}

func (s *ServicePageService) GetServiceTimeSeries(teamUUID string, startMs, endMs int64) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetServiceTimeSeries(teamUUID, startMs, endMs)
}
