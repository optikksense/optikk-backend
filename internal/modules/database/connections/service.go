package connections

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetConnectionCountSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ConnectionCountPoint, error) {
	return s.repo.GetConnectionCountSeries(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnectionUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]ConnectionUtilPoint, error) {
	return s.repo.GetConnectionUtilization(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnectionLimits(ctx context.Context, teamID int64, startMs, endMs int64) ([]ConnectionLimits, error) {
	return s.repo.GetConnectionLimits(ctx, teamID, startMs, endMs)
}

func (s *Service) GetPendingRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]PendingRequestsPoint, error) {
	return s.repo.GetPendingRequests(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnectionTimeoutRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ConnectionTimeoutPoint, error) {
	return s.repo.GetConnectionTimeoutRate(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnectionWaitTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionWaitTime(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnectionCreateTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionCreateTime(ctx, teamID, startMs, endMs)
}

func (s *Service) GetConnectionUseTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionUseTime(ctx, teamID, startMs, endMs)
}
