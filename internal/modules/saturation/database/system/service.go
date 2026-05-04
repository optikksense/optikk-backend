package system

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

// GetSystemLatency returns per-(time_bucket, db.operation.name) latency
// percentiles. Repo emits server-side p50/p95/p99 via quantilesTimingMerge
// on spans_1m.latency_state; service is a pass-through.
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
	out := make([]OpsTimeSeries, len(rows))
	for i, r := range rows {
		rate := r.OpsPerSec
		out[i] = OpsTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, OpsPerSec: &rate}
	}
	return out, nil
}

func (s *Service) GetSystemErrors(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetSystemErrors(ctx, teamID, startMs, endMs, dbSystem)
	if err != nil {
		return nil, err
	}
	out := make([]ErrorTimeSeries, len(rows))
	for i, r := range rows {
		rate := r.OpsPerSec
		out[i] = ErrorTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, ErrorsPerSec: &rate}
	}
	return out, nil
}

// GetSystemTopCollectionsByLatency reads server-side-ranked top-20 rows
// (p99 desc, collection_name asc); pass-through DTO mapping.
func (s *Service) GetSystemTopCollectionsByLatency(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	rows, err := s.repo.GetSystemTopCollectionsByLatency(ctx, teamID, startMs, endMs, dbSystem)
	if err != nil {
		return nil, err
	}
	return mapCollections(rows), nil
}

// GetSystemTopCollectionsByVolume reads server-side-ranked top-20 rows
// (ops_per_sec desc, collection_name asc); pass-through DTO mapping.
func (s *Service) GetSystemTopCollectionsByVolume(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	rows, err := s.repo.GetSystemTopCollectionsByVolume(ctx, teamID, startMs, endMs, dbSystem)
	if err != nil {
		return nil, err
	}
	return mapCollections(rows), nil
}

func (s *Service) GetSystemNamespaces(ctx context.Context, teamID, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	return s.repo.GetSystemNamespaces(ctx, teamID, startMs, endMs, dbSystem)
}

func mapCollections(rows []collectionLatencyRawDTO) []SystemCollectionRow {
	out := make([]SystemCollectionRow, len(rows))
	for i, r := range rows {
		p99 := r.P99Ms
		ops := r.OpsPerSec
		out[i] = SystemCollectionRow{
			CollectionName: r.CollectionName,
			P99Ms:          &p99,
			OpsPerSec:      &ops,
		}
	}
	return out
}
