package facets

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

func (s *Service) GetFacets(ctx context.Context, f filter.Filters) (Facets, error) {
	return s.repo.Facets(ctx, f)
}
