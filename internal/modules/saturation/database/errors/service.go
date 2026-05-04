package errors

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

func (s *Service) GetErrorsBySystem(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsBySystem(ctx, teamID, startMs, endMs, f)
	return mapErrorRate(rows), err
}

func (s *Service) GetErrorsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByOperation(ctx, teamID, startMs, endMs, f)
	return mapErrorRate(rows), err
}

func (s *Service) GetErrorsByErrorType(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByErrorType(ctx, teamID, startMs, endMs, f)
	return mapErrorRate(rows), err
}

func (s *Service) GetErrorsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByCollection(ctx, teamID, startMs, endMs, f)
	return mapErrorRate(rows), err
}

func (s *Service) GetErrorsByResponseStatus(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByResponseStatus(ctx, teamID, startMs, endMs, f)
	return mapErrorRate(rows), err
}

// GetErrorRatio passes through SQL-emitted percentages. has_data == 0
// means "no traffic in this bucket" → expose nil to keep the prior
// JSON shape (NULL on no traffic, 0.0 on real-zero-error traffic).
func (s *Service) GetErrorRatio(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorRatioPoint, error) {
	rows, err := s.repo.GetErrorRatio(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	out := make([]ErrorRatioPoint, len(rows))
	for i, r := range rows {
		var pct *float64
		if r.HasData != 0 {
			v := r.ErrorRatioPct
			pct = &v
		}
		out[i] = ErrorRatioPoint{TimeBucket: r.TimeBucket, ErrorRatioPct: pct}
	}
	return out, nil
}

// mapErrorRate is a pure DTO translation — rates are already per-second
// (SQL emits `sum(error_count) / @bucketGrainSec`).
func mapErrorRate(rows []errorRawDTO) []ErrorTimeSeries {
	if rows == nil {
		return nil
	}
	out := make([]ErrorTimeSeries, len(rows))
	for i, r := range rows {
		rate := r.ErrorsPerSec
		out[i] = ErrorTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, ErrorsPerSec: &rate}
	}
	return out
}
