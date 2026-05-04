package volume

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

func (s *Service) GetOpsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsBySystem(ctx, teamID, startMs, endMs, f)
	return mapOpsRate(rows), err
}

func (s *Service) GetOpsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsByOperation(ctx, teamID, startMs, endMs, f)
	return mapOpsRate(rows), err
}

func (s *Service) GetOpsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsByCollection(ctx, teamID, startMs, endMs, f)
	return mapOpsRate(rows), err
}

func (s *Service) GetOpsByNamespace(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsByNamespace(ctx, teamID, startMs, endMs, f)
	return mapOpsRate(rows), err
}

// GetReadVsWrite is pass-through; rates are computed server-side.
func (s *Service) GetReadVsWrite(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ReadWritePoint, error) {
	rows, err := s.repo.GetReadVsWrite(ctx, teamID, startMs, endMs, f)
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

// mapOpsRate is a pure DTO translation — rates are already per-second
// (SQL emits `sum(request_count) / @bucketGrainSec`).
func mapOpsRate(rows []opsRawDTO) []OpsTimeSeries {
	if rows == nil {
		return nil
	}
	out := make([]OpsTimeSeries, len(rows))
	for i, r := range rows {
		rate := r.OpsPerSec
		out[i] = OpsTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, OpsPerSec: &rate}
	}
	return out
}
