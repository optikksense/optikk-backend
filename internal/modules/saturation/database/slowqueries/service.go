package slowqueries

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetSlowQueryPatterns(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]SlowQueryPattern, error) {
	rows, err := s.repo.GetSlowQueryPatterns(ctx, teamID, startMs, endMs, f, limit)
	if err != nil {
		return nil, err
	}
	out := make([]SlowQueryPattern, len(rows))
	for i, r := range rows {
		p50, p95, p99 := float64(r.P50Ms), float64(r.P95Ms), float64(r.P99Ms)
		out[i] = SlowQueryPattern{
			QueryText:      r.QueryText,
			CollectionName: r.CollectionName,
			P50Ms:          &p50,
			P95Ms:          &p95,
			P99Ms:          &p99,
			CallCount:      int64(r.CallCount),  //nolint:gosec
			ErrorCount:     int64(r.ErrorCount), //nolint:gosec
		}
	}
	return out, nil
}
