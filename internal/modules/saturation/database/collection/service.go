package collection

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

func (s *Service) GetCollectionLatency(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetCollectionLatency(ctx, teamID, startMs, endMs, collection, f)
	if err != nil {
		return nil, err
	}
	out := make([]LatencyTimeSeries, len(rows))
	for i, r := range rows {
		p50, p95, p99 := r.P50Ms, r.P95Ms, r.P99Ms
		out[i] = LatencyTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, P50Ms: &p50, P95Ms: &p95, P99Ms: &p99}
	}
	return out, nil
}

func (s *Service) GetCollectionOps(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetCollectionOps(ctx, teamID, startMs, endMs, collection, f)
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

func (s *Service) GetCollectionErrors(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetCollectionErrors(ctx, teamID, startMs, endMs, collection, f)
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

func (s *Service) GetCollectionQueryTexts(ctx context.Context, teamID, startMs, endMs int64, collection string, f filter.Filters, limit int) ([]CollectionTopQuery, error) {
	rows, err := s.repo.GetCollectionQueryTexts(ctx, teamID, startMs, endMs, collection, f, limit)
	if err != nil {
		return nil, err
	}
	out := make([]CollectionTopQuery, len(rows))
	for i, r := range rows {
		p99 := r.P99Ms
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
	out := make([]ReadWritePoint, len(rows))
	for i, r := range rows {
		read := r.ReadOpsPerSec
		write := r.WriteOpsPerSec
		out[i] = ReadWritePoint{TimeBucket: r.TimeBucket, ReadOpsPerSec: &read, WriteOpsPerSec: &write}
	}
	return out, nil
}
