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
	return foldErrorRate(rows, startMs, endMs), err
}

func (s *Service) GetErrorsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByOperation(ctx, teamID, startMs, endMs, f)
	return foldErrorRate(rows, startMs, endMs), err
}

func (s *Service) GetErrorsByErrorType(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByErrorType(ctx, teamID, startMs, endMs, f)
	return foldErrorRate(rows, startMs, endMs), err
}

func (s *Service) GetErrorsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByCollection(ctx, teamID, startMs, endMs, f)
	return foldErrorRate(rows, startMs, endMs), err
}

func (s *Service) GetErrorsByResponseStatus(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorTimeSeries, error) {
	rows, err := s.repo.GetErrorsByResponseStatus(ctx, teamID, startMs, endMs, f)
	return foldErrorRate(rows, startMs, endMs), err
}

func (s *Service) GetErrorRatio(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ErrorRatioPoint, error) {
	rows, err := s.repo.GetErrorRatio(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	out := make([]ErrorRatioPoint, len(rows))
	for i, r := range rows {
		var pct *float64
		if r.TotalCount > 0 {
			v := float64(r.ErrCount) / float64(r.TotalCount) * 100.0
			pct = &v
		}
		out[i] = ErrorRatioPoint{TimeBucket: r.TimeBucket, ErrorRatioPct: pct}
	}
	return out, nil
}

func foldErrorRate(rows []errorRawDTO, startMs, endMs int64) []ErrorTimeSeries {
	if rows == nil {
		return nil
	}
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]ErrorTimeSeries, len(rows))
	for i, r := range rows {
		rate := float64(r.ErrCount) / bucketSec
		out[i] = ErrorTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, ErrorsPerSec: &rate}
	}
	return out
}
