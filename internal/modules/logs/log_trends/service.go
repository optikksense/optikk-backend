package log_trends //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

// Summary is the in-process include for explorer's /logs/query.
func (s *Service) Summary(ctx context.Context, f filter.Filters) (models.Summary, error) {
	row, err := s.repo.Summary(ctx, f)
	if err != nil {
		return models.Summary{}, fmt.Errorf("logs.Summary: %w", err)
	}
	return models.Summary{Total: row.Total, Errors: row.Errors, Warns: row.Warns}, nil
}

// Trend is the in-process include for explorer's /logs/query.
func (s *Service) Trend(ctx context.Context, f filter.Filters, step string) ([]models.TrendBucket, error) {
	stepMin := resolveStepMinutes(step, 1, f.StartMs, f.EndMs)
	rows, err := s.repo.Trend(ctx, f, stepMin)
	if err != nil {
		return nil, fmt.Errorf("logs.Trend: %w", err)
	}
	return mapTrend(rows, stepMin), nil
}

// ComputeResponse drives the public POST /logs/trends endpoint.
func (s *Service) ComputeResponse(ctx context.Context, f filter.Filters, step string) (Response, error) {
	sum, err := s.Summary(ctx, f)
	if err != nil {
		return Response{}, err
	}
	trend, err := s.Trend(ctx, f, step)
	if err != nil {
		return Response{}, err
	}
	return Response{Summary: sum, Trend: trend}, nil
}

func mapTrend(rows []TrendRow, stepMin int64) []models.TrendBucket {
	out := make([]models.TrendBucket, 0, len(rows))
	for _, r := range rows {
		out = append(out, models.TrendBucket{
			TimeBucket: formatBucket(r.Timestamp, stepMin),
			Severity:   r.SeverityBucket,
			Count:      r.Count,
		})
	}
	return out
}

func resolveStepMinutes(step string, tierStepMin int64, startMs, endMs int64) int64 {
	s := strings.TrimSpace(step)
	if s == "" {
		return adaptiveStep(tierStepMin, startMs, endMs)
	}
	explicit, ok := map[string]int64{"1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 1440}[s]
	if !ok {
		return adaptiveStep(tierStepMin, startMs, endMs)
	}
	if explicit < tierStepMin {
		return tierStepMin
	}
	return explicit
}

func adaptiveStep(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var desired int64
	switch {
	case hours <= 3:
		desired = 1
	case hours <= 24:
		desired = 5
	case hours <= 168:
		desired = 60
	default:
		desired = 1440
	}
	if tierStepMin > desired {
		return tierStepMin
	}
	return desired
}

// formatBucket truncates ts to the chosen stepMin and formats `YYYY-MM-DD HH:MM:SS`.
func formatBucket(ts time.Time, stepMin int64) string {
	utc := ts.UTC()
	if stepMin >= 1440 {
		utc = time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
	} else {
		secs := stepMin * 60
		utc = utc.Truncate(time.Duration(secs) * time.Second)
	}
	return utc.Format("2006-01-02 15:04:05")
}
