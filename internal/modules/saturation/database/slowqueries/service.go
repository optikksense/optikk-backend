package slowqueries

import (
	"context"
	"sort"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	"github.com/Optikk-Org/optikk-backend/internal/shared/quantile"
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
		p50 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.50)
		p95 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.95)
		p99 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.99)
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
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]SlowCollectionRow, len(rows))
	for i, r := range rows {
		p99 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.99)
		ops := float64(r.CallCount) / bucketSec
		var errRate *float64
		if r.CallCount > 0 {
			v := float64(r.ErrorCount) / float64(r.CallCount) * 100.0
			errRate = &v
		}
		out[i] = SlowCollectionRow{
			CollectionName: r.CollectionName,
			P99Ms:          &p99,
			OpsPerSec:      &ops,
			ErrorRate:      errRate,
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return derefOrZero(out[i].P99Ms) > derefOrZero(out[j].P99Ms)
	})
	if len(out) > 50 {
		out = out[:50]
	}
	return out, nil
}

func (s *Service) GetSlowQueryRate(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	rows, err := s.repo.GetSlowQueryRate(ctx, teamID, startMs, endMs, f, thresholdMs)
	if err != nil {
		return nil, err
	}
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]SlowRatePoint, len(rows))
	for i, r := range rows {
		rate := float64(r.SlowCount) / bucketSec
		out[i] = SlowRatePoint{TimeBucket: r.TimeBucket, SlowPerSec: &rate}
	}
	return out, nil
}

func (s *Service) GetP99ByQueryText(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters, limit int) ([]P99ByQueryText, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.repo.GetP99ByQueryText(ctx, teamID, startMs, endMs, f, limit)
	if err != nil {
		return nil, err
	}
	out := make([]P99ByQueryText, len(rows))
	for i, r := range rows {
		p99 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.99)
		out[i] = P99ByQueryText{QueryText: r.QueryText, P99Ms: &p99}
	}
	sort.Slice(out, func(i, j int) bool {
		return derefOrZero(out[i].P99Ms) > derefOrZero(out[j].P99Ms)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func derefOrZero(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
