package errors

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

func (s *Service) GetErrorsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsBySystem(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetErrorsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByOperation(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetErrorsByErrorType(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByErrorType(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetErrorsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByCollection(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetErrorsByResponseStatus(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetErrorsByResponseStatus(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetErrorRatio(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ErrorRatioPoint, error) {
	return s.repo.GetErrorRatio(ctx, teamID, startMs, endMs, f)
}
