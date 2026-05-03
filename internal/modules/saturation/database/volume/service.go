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
	return foldOpsRate(rows, startMs, endMs), err
}

func (s *Service) GetOpsByOperation(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsByOperation(ctx, teamID, startMs, endMs, f)
	return foldOpsRate(rows, startMs, endMs), err
}

func (s *Service) GetOpsByCollection(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsByCollection(ctx, teamID, startMs, endMs, f)
	return foldOpsRate(rows, startMs, endMs), err
}

func (s *Service) GetOpsByNamespace(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]OpsTimeSeries, error) {
	rows, err := s.repo.GetOpsByNamespace(ctx, teamID, startMs, endMs, f)
	return foldOpsRate(rows, startMs, endMs), err
}

// GetReadVsWrite converts the read/write count split into per-second rates.
func (s *Service) GetReadVsWrite(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) ([]ReadWritePoint, error) {
	rows, err := s.repo.GetReadVsWrite(ctx, teamID, startMs, endMs, f)
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

// foldOpsRate is the shared count→rate conversion every per-group ops
// query runs through. count() per 5-minute bucket / display-grain seconds.
func foldOpsRate(rows []opsRawDTO, startMs, endMs int64) []OpsTimeSeries {
	if rows == nil {
		return nil
	}
	bucketSec := filter.BucketWidthSeconds(startMs, endMs)
	out := make([]OpsTimeSeries, len(rows))
	for i, r := range rows {
		rate := float64(r.OpCount) / bucketSec
		out[i] = OpsTimeSeries{TimeBucket: r.TimeBucket, GroupBy: r.GroupBy, OpsPerSec: &rate}
	}
	return out
}
