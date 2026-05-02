package log_trends //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Summary powers POST /api/v1/logs/summary.
func (s *Service) Summary(ctx context.Context, f filter.Filters) (models.Summary, error) {
	row, err := s.repo.Summary(ctx, f)
	if err != nil {
		return models.Summary{}, fmt.Errorf("logs.Summary: %w", err)
	}
	return models.Summary{Total: row.Total, Errors: row.Errors, Warns: row.Warns}, nil
}

// Trend powers POST /api/v1/logs/trend. Buckets are returned at display grain
// (timebucket.DisplayGrain), grouped server-side via toStartOfInterval.
func (s *Service) Trend(ctx context.Context, f filter.Filters) ([]models.TrendBucket, error) {
	rows, err := s.repo.Trend(ctx, f)
	if err != nil {
		return nil, fmt.Errorf("logs.Trend: %w", err)
	}
	return mapTrend(rows), nil
}

// mapTrend is a 1:1 shape mapper — repo rows are already grouped per
// (ts_bucket, severity_bucket) and ordered ASC. ts_bucket is UInt32
// Unix-seconds; format Go-side instead of paying for SQL date functions.
func mapTrend(rows []TrendRow) []models.TrendBucket {
	out := make([]models.TrendBucket, len(rows))
	for i, r := range rows {
		out[i] = models.TrendBucket{
			TimeBucket: time.Unix(int64(r.TsBucket), 0).UTC().Format("2006-01-02 15:04:05"),
			Severity:   r.SeverityBucket,
			Count:      r.Count,
		}
	}
	return out
}
