package log_facets //nolint:revive,stylecheck

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Compute fans out 5 small SQL queries in parallel — one GROUP BY per
// resource dim against observability.logs_resource (top-50 per dim) and one
// GROUP BY severity_bucket against observability.logs (6-row result). Each
// goroutine writes its slice directly into the Facets struct; no Go-side
// fold/sort needed because CH already returns top-N sorted.
func (s *Service) Compute(ctx context.Context, f filter.Filters) (models.Facets, error) {
	var fc models.Facets
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		v, err := s.repo.TopValues(gctx, f, "service")
		fc.Service = v
		return err
	})
	g.Go(func() error {
		v, err := s.repo.TopValues(gctx, f, "host")
		fc.Host = v
		return err
	})
	g.Go(func() error {
		v, err := s.repo.TopValues(gctx, f, "pod")
		fc.Pod = v
		return err
	})
	g.Go(func() error {
		v, err := s.repo.TopValues(gctx, f, "environment")
		fc.Environment = v
		return err
	})
	g.Go(func() error {
		v, err := s.repo.SeverityCounts(gctx, f)
		fc.Severity = v
		return err
	})

	if err := g.Wait(); err != nil {
		return models.Facets{}, fmt.Errorf("logs.Facets: %w", err)
	}
	return fc, nil
}

// ComputeResponse wraps Compute as a wire Response for the public endpoint.
func (s *Service) ComputeResponse(ctx context.Context, f filter.Filters) (Response, error) {
	fc, err := s.Compute(ctx, f)
	if err != nil {
		return Response{}, err
	}
	return Response{Facets: fc}, nil
}
