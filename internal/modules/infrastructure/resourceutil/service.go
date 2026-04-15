package resourceutil //nolint:misspell

import "context"

// Service delegates to the composite Repository.
type Service interface {
	GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]InstanceResource, error)
}

type resourceUtilService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &resourceUtilService{repo: repo}
}

func (s *resourceUtilService) GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgCPU(ctx, teamID, startMs, endMs)
}
func (s *resourceUtilService) GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgMemory(ctx, teamID, startMs, endMs)
}
func (s *resourceUtilService) GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgNetwork(ctx, teamID, startMs, endMs)
}
func (s *resourceUtilService) GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgConnPool(ctx, teamID, startMs, endMs)
}
func (s *resourceUtilService) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetCPUUsagePercentage(ctx, teamID, startMs, endMs)
}
func (s *resourceUtilService) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetMemoryUsagePercentage(ctx, teamID, startMs, endMs)
}
func (s *resourceUtilService) GetResourceUsageByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceResource, error) {
	return s.repo.GetResourceUsageByService(ctx, teamID, startMs, endMs)
}
func (s *resourceUtilService) GetResourceUsageByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]InstanceResource, error) {
	return s.repo.GetResourceUsageByInstance(ctx, teamID, startMs, endMs)
}
