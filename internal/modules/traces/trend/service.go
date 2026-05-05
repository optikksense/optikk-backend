package trend

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

func (s *Service) GetTrend(ctx context.Context, f filter.Filters) ([]TrendBucket, error) {
	return s.repo.Trend(ctx, f)
}
