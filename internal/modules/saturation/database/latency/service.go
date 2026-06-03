package latency

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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

func foldLatency(rows []latencyRawDTO) []LatencyTimeSeries {
	if rows == nil {
		return nil
	}
	out := make([]LatencyTimeSeries, len(rows))
	for i, r := range rows {
		p50, p95, p99 := float64(r.P50Ms), float64(r.P95Ms), float64(r.P99Ms)
		out[i] = LatencyTimeSeries{
			TimeBucket: timebucket.FormatDisplayBucket(r.BucketAt),
			GroupBy:    r.GroupBy,
			P50Ms:      &p50,
			P95Ms:      &p95,
			P99Ms:      &p99,
		}
	}
	return out
}
