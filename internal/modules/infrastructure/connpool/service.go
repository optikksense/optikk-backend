package connpool

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (MetricValue, error) {
	return s.repo.GetAvgConnPool(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnPoolByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]ConnPoolServiceMetric, error) {
	return s.repo.GetConnPoolByService(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnPoolByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]ConnPoolInstanceMetric, error) {
	return s.repo.GetConnPoolByInstance(ctx, teamID, startMs, endMs)
}
