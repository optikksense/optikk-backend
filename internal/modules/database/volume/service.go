package volume

import (
	"context"

	shared "github.com/observability/observability-backend-go/internal/modules/database/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetOpsBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsBySystem(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetOpsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsByOperation(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetOpsByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsByCollection(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ReadWritePoint, error) {
	return s.repo.GetReadVsWrite(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetOpsByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetOpsByNamespace(ctx, teamID, startMs, endMs, f)
}
