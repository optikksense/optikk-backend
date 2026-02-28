package service

import (
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation/model"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/resource_utilisation/store"
)

// Service encapsulates the business logic for the resource utilisation module.
type Service interface {
	GetAvgCPU(teamUUID string, startMs, endMs int64) (model.MetricValue, error)
	GetAvgMemory(teamUUID string, startMs, endMs int64) (model.MetricValue, error)
	GetAvgNetwork(teamUUID string, startMs, endMs int64) (model.MetricValue, error)
	GetAvgConnPool(teamUUID string, startMs, endMs int64) (model.MetricValue, error)
	GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]model.ResourceBucket, error)
	GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]model.ResourceBucket, error)
	GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]model.ServiceResource, error)
	GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]model.InstanceResource, error)
}

// ResourceUtilisationService provides business logic orchestration.
type ResourceUtilisationService struct {
	repo store.Repository
}

// NewService creates a new ResourceUtilisationService.
func NewService(repo store.Repository) Service {
	return &ResourceUtilisationService{repo: repo}
}

func (s *ResourceUtilisationService) GetAvgCPU(teamUUID string, startMs, endMs int64) (model.MetricValue, error) {
	return s.repo.GetAvgCPU(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgMemory(teamUUID string, startMs, endMs int64) (model.MetricValue, error) {
	return s.repo.GetAvgMemory(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgNetwork(teamUUID string, startMs, endMs int64) (model.MetricValue, error) {
	return s.repo.GetAvgNetwork(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetAvgConnPool(teamUUID string, startMs, endMs int64) (model.MetricValue, error) {
	return s.repo.GetAvgConnPool(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]model.ResourceBucket, error) {
	return s.repo.GetCPUUsagePercentage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]model.ResourceBucket, error) {
	return s.repo.GetMemoryUsagePercentage(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]model.ServiceResource, error) {
	return s.repo.GetResourceUsageByService(teamUUID, startMs, endMs)
}

func (s *ResourceUtilisationService) GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]model.InstanceResource, error) {
	return s.repo.GetResourceUsageByInstance(teamUUID, startMs, endMs)
}
