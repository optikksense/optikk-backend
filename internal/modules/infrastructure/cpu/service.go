package cpu

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetCPUTime(ctx, teamID, startMs, endMs)
}

func (s *Service) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	return s.repo.GetCPUUsagePercentage(ctx, teamID, startMs, endMs)
}

func (s *Service) GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (LoadAverageResult, error) {
	return s.repo.GetLoadAverage(ctx, teamID, startMs, endMs)
}

func (s *Service) GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	return s.repo.GetProcessCount(ctx, teamID, startMs, endMs)
}

func (s *Service) GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgCPU(ctx, teamID, startMs, endMs)
}

func (s *Service) GetCPUByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUServiceMetric, error) {
	return s.repo.GetCPUByService(ctx, teamID, startMs, endMs)
}

func (s *Service) GetCPUByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]CPUInstanceMetric, error) {
	return s.repo.GetCPUByInstance(ctx, teamID, startMs, endMs)
}
