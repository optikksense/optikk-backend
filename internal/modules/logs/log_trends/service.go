package log_trends //nolint:revive,stylecheck

import (
	"context"
	"fmt"
	"sort"
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
	rows, err := s.repo.Trend(ctx, f)
	if err != nil {
		return nil, fmt.Errorf("logs.Trend: %w", err)
	}
	stepMin := resolveStepMinutes(step, 1, f.StartMs, f.EndMs)
	return foldTrend(rows, stepMin), nil
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

func foldTrend(rows []TrendRawRow, stepMin int64) []models.TrendBucket {
	type key struct {
		bucket   string
		severity uint8
	}
	counts := make(map[key]uint64)
	for _, r := range rows {
		bucket := formatBucket(r.Timestamp, stepMin)
		counts[key{bucket: bucket, severity: r.SeverityBucket}]++
	}

	keys := make([]key, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucket == keys[j].bucket {
			return keys[i].severity < keys[j].severity
		}
		return keys[i].bucket < keys[j].bucket
	})

	out := make([]models.TrendBucket, 0, len(keys))
	for _, k := range keys {
		out = append(out, models.TrendBucket{
			TimeBucket: k.bucket,
			Severity:   k.severity,
			Count:      counts[k],
		})
	}
	return out
}

// resolveStepMinutes picks a CH-friendly step size in minutes, honoring an
// explicit token when provided and adaptive defaults otherwise. Never returns
// less than tierStepMin so any future rollup tier alignment is preserved.
// Adaptive: ≤3h → 1m, ≤24h → 5m, ≤7d → 1h, else 1d.
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
