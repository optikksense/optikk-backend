package analytics

import (
	"context"
	"fmt"
)

const (
	maxGroupByDimensions = 3
	maxAggregations      = 5
	maxTimeRangeMs       = 30 * 24 * 60 * 60 * 1000
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// RunQuery validates the analytics query and executes it.
func (s *Service) RunQuery(ctx context.Context, teamID int64, q AnalyticsQuery) (*AnalyticsResult, error) {
	if err := validate(q); err != nil {
		return nil, err
	}
	return s.repo.Execute(ctx, teamID, q)
}

func validate(q AnalyticsQuery) error {
	if len(q.GroupBy) == 0 {
		return fmt.Errorf("at least one groupBy dimension is required")
	}
	if len(q.GroupBy) > maxGroupByDimensions {
		return fmt.Errorf("maximum %d groupBy dimensions allowed", maxGroupByDimensions)
	}
	if len(q.Aggregations) == 0 {
		return fmt.Errorf("at least one aggregation is required")
	}
	if len(q.Aggregations) > maxAggregations {
		return fmt.Errorf("maximum %d aggregations allowed", maxAggregations)
	}

	for _, dim := range q.GroupBy {
		if _, ok := LookupDimension(dim); !ok {
			return fmt.Errorf("invalid groupBy dimension: %q", dim)
		}
	}

	if q.Filters.StartMs <= 0 {
		return fmt.Errorf("filters.startMs is required")
	}
	if q.Filters.EndMs <= 0 {
		return fmt.Errorf("filters.endMs is required")
	}
	if q.Filters.EndMs <= q.Filters.StartMs {
		return fmt.Errorf("filters.endMs must be after filters.startMs")
	}
	if (q.Filters.EndMs - q.Filters.StartMs) > maxTimeRangeMs {
		return fmt.Errorf("time range cannot exceed 30 days")
	}

	seen := make(map[string]bool, len(q.Aggregations))
	for _, a := range q.Aggregations {
		if a.Alias == "" {
			return fmt.Errorf("aggregation alias is required")
		}
		if seen[a.Alias] {
			return fmt.Errorf("duplicate aggregation alias: %q", a.Alias)
		}
		seen[a.Alias] = true
	}
	return nil
}
