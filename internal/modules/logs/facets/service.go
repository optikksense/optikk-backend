package log_facets //nolint:revive,stylecheck

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Compute fires ONE CH query that returns top-N values for all four resource
// dims (service/host/pod/environment) on observability.logs_resource. Severity
// is a static label list — see models.SeverityLabels — no DB call. Rows are
// folded into the typed Facets struct by `dim`.
func (s *Service) Compute(ctx context.Context, f filter.Filters) (models.Facets, error) {
	rows, err := s.repo.Compute(ctx, f)
	if err != nil {
		return models.Facets{}, err
	}
	fc := models.Facets{Severity: models.SeverityLabels}
	for _, r := range rows {
		fv := models.FacetValue{Value: r.Value, Count: r.Count}
		switch r.Dim {
		case "service":
			fc.Service = append(fc.Service, fv)
		case "host":
			fc.Host = append(fc.Host, fv)
		case "pod":
			fc.Pod = append(fc.Pod, fv)
		case "environment":
			fc.Environment = append(fc.Environment, fv)
		}
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
