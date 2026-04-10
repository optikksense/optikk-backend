package connections

import (
	"context"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetConnectionCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionCountPoint, error) {
	return s.repo.GetConnectionCountSeries(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetConnectionUtilization(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionUtilPoint, error) {
	return s.repo.GetConnectionUtilization(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetConnectionLimits(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionLimits, error) {
	return s.repo.GetConnectionLimits(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetPendingRequests(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PendingRequestsPoint, error) {
	return s.repo.GetPendingRequests(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetConnectionTimeoutRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionTimeoutPoint, error) {
	return s.repo.GetConnectionTimeoutRate(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetConnectionWaitTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionWaitTime(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetConnectionCreateTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionCreateTime(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetConnectionUseTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return s.repo.GetConnectionUseTime(ctx, teamID, startMs, endMs, f)
}
