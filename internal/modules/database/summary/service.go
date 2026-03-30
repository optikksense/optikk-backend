package summary

import (
	"context"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/database/internal/shared"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (SummaryStats, error) {
	return s.repo.GetSummaryStats(ctx, teamID, startMs, endMs, f)
}
