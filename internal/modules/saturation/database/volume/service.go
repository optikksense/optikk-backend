package volume

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

func (s *Service) GetOpsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsBySystem(ctx, teamID, startMs, endMs, f)
	return mapOpsRate(rows), err
}

// mapOpsRate is a pure DTO translation — rates are already per-second
// (SQL emits `sum(request_count) / @bucketGrainSec`).
func mapOpsRate(rows []opsRawDTO) []OpsTimeSeries {
	if rows == nil {
		return nil
	}
	out := make([]OpsTimeSeries, len(rows))
	for i, r := range rows {
		rate := r.OpsPerSec
		out[i] = OpsTimeSeries{TimeBucket: timebucket.FormatDisplayBucket(r.TimeBucket), GroupBy: r.GroupBy, OpsPerSec: &rate}
	}
	return out
}
