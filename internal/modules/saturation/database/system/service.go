package system

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

func (s *Service) GetSystemLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetSystemLatency(ctx, teamID, startMs, endMs, dbSystem, f)
}

func (s *Service) GetSystemOps(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetSystemOps(ctx, teamID, startMs, endMs, dbSystem, f)
}

func (s *Service) GetSystemTopCollectionsByLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	return s.repo.GetSystemTopCollectionsByLatency(ctx, teamID, startMs, endMs, dbSystem)
}

func (s *Service) GetSystemTopCollectionsByVolume(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	return s.repo.GetSystemTopCollectionsByVolume(ctx, teamID, startMs, endMs, dbSystem)
}

func (s *Service) GetSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	return s.repo.GetSystemErrors(ctx, teamID, startMs, endMs, dbSystem)
}

func (s *Service) GetSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	return s.repo.GetSystemNamespaces(ctx, teamID, startMs, endMs, dbSystem)
}
