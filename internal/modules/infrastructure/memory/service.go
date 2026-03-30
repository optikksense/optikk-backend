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
