package log_trends //nolint:revive,stylecheck

import (
	"context"
	"fmt"

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

// Trend powers POST /api/v1/logs/trend. Buckets are returned at display grain,
// grouped server-side via timebucket.DisplayGrainSQL.
func (s *Service) Trend(ctx context.Context, f filter.Filters) ([]models.TrendBucket, error) {
	rows, err := s.repo.Trend(ctx, f)
	if err != nil {
		return nil, fmt.Errorf("logs.Trend: %w", err)
	}
	return mapTrend(rows), nil
}

// mapTrend is a 1:1 shape mapper — repo rows are already grouped per
// time_bucket and ordered ASC, one row per display bucket carrying total +
// per-severity-tier counts. time_bucket scans natively as DateTime; format
// Go-side to match the wire contract.
func mapTrend(rows []TrendRow) []models.TrendBucket {
	out := make([]models.TrendBucket, len(rows))
	for i, r := range rows {
		out[i] = models.TrendBucket{
			TimeBucket: r.TimeBucket.UTC().Format("2006-01-02 15:04:05"),
			Total:      r.Total,
			Error:      r.Error,
			Warn:       r.Warn,
			Info:       r.Info,
			Debug:      r.Debug,
		}
	}
	return out
}
