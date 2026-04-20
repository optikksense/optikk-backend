package collection

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

func (s *Service) GetCollectionLatency(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetCollectionLatency(ctx, teamID, startMs, endMs, collection, f)
}

func (s *Service) GetCollectionOps(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetCollectionOps(ctx, teamID, startMs, endMs, collection, f)
}

func (s *Service) GetCollectionErrors(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetCollectionErrors(ctx, teamID, startMs, endMs, collection, f)
}

func (s *Service) GetCollectionQueryTexts(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters, limit int) ([]CollectionTopQuery, error) {
	return s.repo.GetCollectionQueryTexts(ctx, teamID, startMs, endMs, collection, f, limit)
}

func (s *Service) GetCollectionReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error) {
	return s.repo.GetCollectionReadVsWrite(ctx, teamID, startMs, endMs, collection)
}
