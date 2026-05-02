package latency

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

func (s *Service) GetLatencyBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetLatencyBySystem(ctx, teamID, startMs, endMs, f)
	return foldLatency(rows), err
}

func (s *Service) GetLatencyByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetLatencyByOperation(ctx, teamID, startMs, endMs, f)
	return foldLatency(rows), err
}

func (s *Service) GetLatencyByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetLatencyByCollection(ctx, teamID, startMs, endMs, f)
	return foldLatency(rows), err
}

func (s *Service) GetLatencyByNamespace(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetLatencyByNamespace(ctx, teamID, startMs, endMs, f)
	return foldLatency(rows), err
}

func (s *Service) GetLatencyByServer(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetLatencyByServer(ctx, teamID, startMs, endMs, f)
	return foldLatency(rows), err
}

// GetLatencyHeatmap converts raw counts into per-bucket density (fraction
// of the time-bucket's total). Pass-through count preserved.
func (s *Service) GetLatencyHeatmap(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]LatencyHeatmapBucket, error) {
	rows, err := s.repo.GetLatencyHeatmap(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	totals := map[string]int64{}
	for _, r := range rows {
		totals[r.TimeBucket] += r.Count
	}
	out := make([]LatencyHeatmapBucket, len(rows))
	for i, r := range rows {
		var density float64
		if t := totals[r.TimeBucket]; t > 0 {
			density = float64(r.Count) / float64(t)
		}
		out[i] = LatencyHeatmapBucket{
			TimeBucket:  r.TimeBucket,
			BucketLabel: r.BucketLabel,
			Count:       r.Count,
			Density:     density,
		}
	}
	return out, nil
}

func foldLatency(rows []latencyRawDTO) []LatencyTimeSeries {
	if rows == nil {
		return nil
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
	return out
}
