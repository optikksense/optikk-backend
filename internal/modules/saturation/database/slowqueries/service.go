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
		p50, p95, p99 := r.P50Ms, r.P95Ms, r.P99Ms
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

func (s *Service) GetSlowestCollections(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]SlowCollectionRow, error) {
	rows, err := s.repo.GetSlowestCollections(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	out := make([]SlowCollectionRow, len(rows))
	for i, r := range rows {
		p99 := r.P99Ms
		ops := r.OpsPerSec
		var errRate *float64
		if r.HasCalls != 0 {
			v := r.ErrorRatePct
			errRate = &v
		}
		out[i] = SlowCollectionRow{
			CollectionName: r.CollectionName,
			P99Ms:          &p99,
			OpsPerSec:      &ops,
			ErrorRate:      errRate,
		}
	}
	return out, nil
}

func (s *Service) GetSlowQueryRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	rows, err := s.repo.GetSlowQueryRate(ctx, teamID, startMs, endMs, f, thresholdMs)
	if err != nil {
		return nil, err
	}
	out := make([]SlowRatePoint, len(rows))
	for i, r := range rows {
		rate := r.SlowPerSec
		out[i] = SlowRatePoint{TimeBucket: r.TimeBucket, SlowPerSec: &rate}
	}
	return out, nil
}

func (s *Service) GetP99ByQueryText(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]P99ByQueryText, error) {
	rows, err := s.repo.GetP99ByQueryText(ctx, teamID, startMs, endMs, f, limit)
	if err != nil {
		return nil, err
	}
	out := make([]P99ByQueryText, len(rows))
	for i, r := range rows {
		p99 := r.P99Ms
		out[i] = P99ByQueryText{QueryText: r.QueryText, P99Ms: &p99}
	}
	return out, nil
}
