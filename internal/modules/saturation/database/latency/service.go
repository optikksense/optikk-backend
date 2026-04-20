package latency

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Service struct {
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) *Service {
	return &Service{repo: repo, sketchQ: sketchQ}
}

func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

// fillPercentilesForSystemSeries attaches p50/p95/p99 to time-series rows
// grouped by db.system. The DbOpLatency sketch is keyed by
// system|operation|collection|namespace, so prefix `<system>|` yields a
// per-system merged sketch across the full [start, end] window. We attach
// the same aggregate percentile to every time_bucket for a given system —
// the chart still renders a flat band and the scalar numbers on the tooltip
// stay accurate for the overall window. Per-bucket series can come later
// with a second sketch kind if the UX demands it.
func (s *Service) fillPercentilesForSystemSeries(ctx context.Context, teamID int64, startMs, endMs int64, rows []LatencyTimeSeries) {
	if s.sketchQ == nil || len(rows) == 0 {
		return
	}
	seen := map[string]struct{}{}
	prefixes := make([]string, 0, len(rows))
	for _, r := range rows {
		if _, ok := seen[r.GroupBy]; ok {
			continue
		}
		seen[r.GroupBy] = struct{}{}
		prefixes = append(prefixes, r.GroupBy+"|")
	}
	pcts, err := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.DbOpLatency, teamIDString(teamID), startMs, endMs, prefixes, 0.50, 0.95, 0.99)
	if err != nil {
		return
	}
	for i := range rows {
		if v, ok := pcts[rows[i].GroupBy+"|"]; ok && len(v) == 3 {
			p50 := v[0] * 1000.0
			p95 := v[1] * 1000.0
			p99 := v[2] * 1000.0
			rows[i].P50Ms = &p50
			rows[i].P95Ms = &p95
			rows[i].P99Ms = &p99
		}
	}
}

// The by-operation/by-collection/by-namespace/by-server views group by a
// single dim-tuple axis other than db.system. DbOpLatency's dim is
// system|operation|collection|namespace, so PercentilesByDimPrefix cannot
// address a mid-tuple axis without adding a dedicated sketch kind. Known
// limitation — percentiles stay as the zero placeholders emitted by the
// repo. Adding a per-axis sketch kind is tracked as follow-up.

func (s *Service) GetLatencyBySystem(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetLatencyBySystem(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	s.fillPercentilesForSystemSeries(ctx, teamID, startMs, endMs, rows)
	return rows, nil
}

func (s *Service) GetLatencyByOperation(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByOperation(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyByCollection(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByCollection(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyByNamespace(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByNamespace(ctx, teamID, startMs, endMs, f)
}

func (s *Service) GetLatencyByServer(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyTimeSeries, error) {
	return s.repo.GetLatencyByServer(ctx, teamID, startMs, endMs, f)
}

// classifyHeatmapBucket maps an average latency (in seconds) to the same
// label ladder previously emitted by a SQL conditional chain.
func classifyHeatmapBucket(avgSec float64) string {
	switch {
	case avgSec < 0.001:
		return "< 1ms"
	case avgSec < 0.005:
		return "1–5ms"
	case avgSec < 0.010:
		return "5–10ms"
	case avgSec < 0.025:
		return "10–25ms"
	case avgSec < 0.050:
		return "25–50ms"
	case avgSec < 0.100:
		return "50–100ms"
	case avgSec < 0.250:
		return "100–250ms"
	case avgSec < 0.500:
		return "250–500ms"
	case avgSec < 1.000:
		return "500ms–1s"
	default:
		return "> 1s"
	}
}

// GetLatencyHeatmap rebuilds the (time_bucket, bucket_label, count, density)
// output from raw samples returned by the repository. All classification and
// density math lives here so the SQL in repository.go stays free of banned
// CH combinators and casts.
func (s *Service) GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]LatencyHeatmapBucket, error) {
	samples, err := s.repo.GetLatencyHeatmapSamples(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}

	// First pass: aggregate counts per (time_bucket, bucket_label).
	type key struct {
		timeBucket string
		label      string
	}
	counts := make(map[key]int64, len(samples))
	totals := make(map[string]int64, len(samples))
	order := make([]key, 0, len(samples))
	for _, s := range samples {
		k := key{timeBucket: s.TimeBucket, label: classifyHeatmapBucket(s.AvgSec)}
		if _, exists := counts[k]; !exists {
			order = append(order, k)
		}
		counts[k] += s.Count
		totals[s.TimeBucket] += s.Count
	}

	out := make([]LatencyHeatmapBucket, 0, len(order))
	for _, k := range order {
		c := counts[k]
		density := 0.0
		if t := totals[k.timeBucket]; t > 0 {
			density = float64(c) / float64(t)
		}
		out = append(out, LatencyHeatmapBucket{
			TimeBucket:  k.timeBucket,
			BucketLabel: k.label,
			Count:       c,
			Density:     density,
		})
	}
	return out, nil
}
