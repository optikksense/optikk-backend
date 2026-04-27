package log_facets //nolint:revive,stylecheck

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Compute is the in-process API used by explorer's /logs/query include
// fan-out. Returns the parsed Facets + dropped-clause warnings.
func (s *Service) Compute(ctx context.Context, f querycompiler.Filters) (models.Facets, []string, error) {
	fc, warns, err := s.repo.Facets(ctx, f)
	if err != nil {
		return models.Facets{}, nil, fmt.Errorf("logs.Facets: %w", err)
	}
	return fc, warns, nil
}

// ComputeResponse wraps Compute as a wire Response for the public endpoint.
func (s *Service) ComputeResponse(ctx context.Context, f querycompiler.Filters) (Response, error) {
	fc, warns, err := s.Compute(ctx, f)
	if err != nil {
		return Response{}, err
	}
	return Response{Facets: fc, Warnings: warns}, nil
}
