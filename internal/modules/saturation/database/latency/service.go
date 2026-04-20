package latency

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

func (s *Service) GetLatencyBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyBySystem(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByOperation(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByCollection(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByNamespace(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyByServer(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByServer(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyHeatmapBucket, error) {
	return s.repo.GetLatencyHeatmap(ctx, teamID, startMs, endMs, f)
}
