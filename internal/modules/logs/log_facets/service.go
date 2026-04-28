package log_facets //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

const facetTopN = 50

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Compute is the in-process API used by explorer's /logs/query include
// fan-out.
func (s *Service) Compute(ctx context.Context, f filter.Filters) (models.Facets, error) {
	rows, err := s.repo.Facets(ctx, f)
	if err != nil {
		return models.Facets{}, fmt.Errorf("logs.Facets: %w", err)
	}
	return foldFacets(rows), nil
}

// ComputeResponse wraps Compute as a wire Response for the public endpoint.
func (s *Service) ComputeResponse(ctx context.Context, f filter.Filters) (Response, error) {
	fc, err := s.Compute(ctx, f)
	if err != nil {
		return Response{}, err
	}
	return Response{Facets: fc}, nil
}

func foldFacets(rows []FacetRow) models.Facets {
	severity := make(map[string]uint64)
	service := make(map[string]uint64)
	host := make(map[string]uint64)
	pod := make(map[string]uint64)
	environment := make(map[string]uint64)
	for _, r := range rows {
		severity[strconv.Itoa(int(r.SeverityBucket))]++
		if r.Service != "" {
			service[r.Service]++
		}
		if r.Host != "" {
			host[r.Host]++
		}
		if r.Pod != "" {
			pod[r.Pod]++
		}
		if r.Environment != "" {
			environment[r.Environment]++
		}
	}
	return models.Facets{
		Severity:    topFacetValues(severity, facetTopN),
		Service:     topFacetValues(service, facetTopN),
		Host:        topFacetValues(host, facetTopN),
		Pod:         topFacetValues(pod, facetTopN),
		Environment: topFacetValues(environment, facetTopN),
	}
}

func topFacetValues(counts map[string]uint64, limit int) []models.FacetValue {
	type pair struct {
		value string
		count uint64
	}
	pairs := make([]pair, 0, len(counts))
	for value, count := range counts {
		pairs = append(pairs, pair{value: value, count: count})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count == pairs[j].count {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].count > pairs[j].count
	})
	if len(pairs) > limit {
		pairs = pairs[:limit]
	}
	out := make([]models.FacetValue, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, models.FacetValue{Value: p.value, Count: p.count})
	}
	return out
}
