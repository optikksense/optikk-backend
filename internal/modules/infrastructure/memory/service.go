package memory

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetMemoryUsage(ctx, teamID, startMs, endMs)
}

func (s *Service) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetMemoryUsagePercentage(ctx, teamID, startMs, endMs)
}

func (s *Service) GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetSwapUsage(ctx, teamID, startMs, endMs)
}

func (s *Service) GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgMemory(ctx, teamID, startMs, endMs)
}

func (s *Service) GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return s.repo.GetMemoryByService(ctx, teamID, serviceName, startMs, endMs)
}

func (s *Service) GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return s.repo.GetMemoryByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
