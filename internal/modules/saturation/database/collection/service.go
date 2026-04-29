package collection

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	"github.com/Optikk-Org/optikk-backend/internal/shared/quantile"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetCollectionLatency(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetCollectionLatency(ctx, teamID, startMs, endMs, collection, f)
	if err != nil {
		return nil, err
	}
	out := make([]LatencyTimeSeries, len(rows))
	for i, r := range rows {
		p50 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.50)
		p95 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.95)
		p99 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.99)
		out[i] = LatencyTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, P50Ms: &p50, P95Ms: &p95, P99Ms: &p99}
	}
	return out, nil
}

func (s *Service) GetCollectionOps(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetCollectionOps(ctx, teamID, startMs, endMs, collection, f)
	if err != nil {
		return nil, err
	}
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]OpsTimeSeries, len(rows))
	for i, r := range rows {
		rate := float64(r.Count) / bucketSec
		out[i] = OpsTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, OpsPerSec: &rate}
	}
	return out, nil
}

func (s *Service) GetCollectionErrors(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetCollectionErrors(ctx, teamID, startMs, endMs, collection, f)
	if err != nil {
		return nil, err
	}
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]ErrorTimeSeries, len(rows))
	for i, r := range rows {
		rate := float64(r.Count) / bucketSec
		out[i] = ErrorTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, ErrorsPerSec: &rate}
	}
	return out, nil
}

func (s *Service) GetCollectionQueryTexts(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters, limit int) ([]CollectionTopQuery, error) {
	rows, err := s.repo.GetCollectionQueryTexts(ctx, teamID, startMs, endMs, collection, f, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CollectionTopQuery, len(rows))
	for i, r := range rows {
		p99 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.99)
		out[i] = CollectionTopQuery{
			QueryText:  r.QueryText,
			P99Ms:      &p99,
			CallCount:  int64(r.CallCount),  //nolint:gosec // domain-bounded
			ErrorCount: int64(r.ErrorCount), //nolint:gosec
		}
	}
	return out, nil
}

func (s *Service) GetCollectionReadVsWrite(ctx context.Context, teamID, startMs, endMs int64, collection string) ([]ReadWritePoint, error) {
	rows, err := s.repo.GetCollectionReadVsWrite(ctx, teamID, startMs, endMs, collection)
	if err != nil {
		return nil, err
	}
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]ReadWritePoint, len(rows))
	for i, r := range rows {
		read := float64(r.ReadCount) / bucketSec
		write := float64(r.WriteCount) / bucketSec
		out[i] = ReadWritePoint{TimeBucket: r.TimeBucket, ReadOpsPerSec: &read, WriteOpsPerSec: &write}
	}
	return out, nil
}
