package system

import (
	"context"
	"sort"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// GetSystemLatency returns per-(time_bucket, db.operation.name) latency
// percentiles. Repo emits histogram bucket counts; service interpolates
// p50/p95/p99 Go-side via quantile.FromHistogram.
func (s *Service) GetSystemLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetSystemLatency(ctx, teamID, startMs, endMs, dbSystem, f)
	if err != nil {
		return nil, err
	}
	out := make([]LatencyTimeSeries, len(rows))
	for i, r := range rows {
		p50, p95, p99 := r.P50Ms, r.P95Ms, r.P99Ms
		out[i] = LatencyTimeSeries{
			TimeBucket: r.TimeBucket,
			GroupBy:    r.GroupBy,
			P50Ms:      &p50,
			P95Ms:      &p95,
			P99Ms:      &p99,
		}
	}
	return out, nil
}

func (s *Service) GetSystemOps(ctx context.Context, teamID, startMs, endMs int64, dbSystem string, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetSystemOps(ctx, teamID, startMs, endMs, dbSystem, f)
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

func (s *Service) GetSystemErrors(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetSystemErrors(ctx, teamID, startMs, endMs, dbSystem)
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

// GetSystemTopCollectionsByLatency interpolates per-collection p99 from
// the bucket-count histogram and orders by p99 desc, top 20.
func (s *Service) GetSystemTopCollectionsByLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	rows, err := s.repo.GetSystemTopCollectionsByLatency(ctx, teamID, startMs, endMs, dbSystem)
	if err != nil {
		return nil, err
	}
	collections := foldCollections(rows, startMs, endMs)
	sort.Slice(collections, func(i, j int) bool {
		return derefOrZero(collections[i].P99Ms) > derefOrZero(collections[j].P99Ms)
	})
	if len(collections) > 20 {
		collections = collections[:20]
	}
	return collections, nil
}

// GetSystemTopCollectionsByVolume orders by ops_per_sec desc, top 20.
func (s *Service) GetSystemTopCollectionsByVolume(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	rows, err := s.repo.GetSystemTopCollectionsByVolume(ctx, teamID, startMs, endMs, dbSystem)
	if err != nil {
		return nil, err
	}
	collections := foldCollections(rows, startMs, endMs)
	sort.Slice(collections, func(i, j int) bool {
		return derefOrZero(collections[i].OpsPerSec) > derefOrZero(collections[j].OpsPerSec)
	})
	if len(collections) > 20 {
		collections = collections[:20]
	}
	return collections, nil
}

func (s *Service) GetSystemNamespaces(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	return s.repo.GetSystemNamespaces(ctx, teamID, startMs, endMs, dbSystem)
}

func foldCollections(rows []collectionLatencyRawDTO, startMs, endMs int64) []SystemCollectionRow {
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]SystemCollectionRow, len(rows))
	for i, r := range rows {
		p99 := r.P99Ms
		ops := float64(r.Count) / bucketSec
		out[i] = SystemCollectionRow{
			CollectionName: r.CollectionName,
			P99Ms:          &p99,
			OpsPerSec:      &ops,
		}
	}
	return out
}

func derefOrZero(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
