package service

import (
	"github.com/observability/observability-backend-go/internal/modules/overview/overview/model"
	"github.com/observability/observability-backend-go/internal/modules/overview/overview/store"
)

// Service encapsulates the business logic for the overview module.
type Service interface {
	GetSummary(teamUUID string, startMs, endMs int64) (model.Summary, error)
	GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetServices(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error)
	GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]model.EndpointMetric, error)
	GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
}

// OverviewService provides business logic orchestration for overview dashboards.
type OverviewService struct {
	repo store.Repository
}

// NewService creates a new OverviewService.
func NewService(repo store.Repository) Service {
	return &OverviewService{repo: repo}
}

func (s *OverviewService) GetSummary(teamUUID string, startMs, endMs int64) (model.Summary, error) {
	return s.repo.GetSummary(teamUUID, startMs, endMs)
}

func (s *OverviewService) GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetTimeSeries(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetServices(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error) {
	return s.repo.GetServices(teamUUID, startMs, endMs)
}

func (s *OverviewService) GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]model.EndpointMetric, error) {
	return s.repo.GetEndpointMetrics(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	return s.repo.GetEndpointTimeSeries(teamUUID, startMs, endMs, serviceName)
}
