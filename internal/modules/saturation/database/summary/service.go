package summary

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

func (s *Service) GetSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (SummaryStats, error) {
	main, err := s.repo.GetSummaryMain(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return SummaryStats{}, err
	}

	durationSecs := max(float64(endMs-startMs)/1000.0, 1)

	// Avg latency (ms) — sum/count in Go. hist_sum is in seconds.
	var avgLatencyMs *float64
	if main.LatencyCount > 0 {
		v := (main.LatencySum / float64(main.LatencyCount)) * 1000.0
		avgLatencyMs = &v
	}

	// P95 / P99 — DbOpLatency sketch merged across every dim for this tenant.
	var p95Ms, p99Ms *float64
	if s.sketchQ != nil {
		pcts, _ := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.DbOpLatency, teamIDString(teamID), startMs, endMs, []string{""}, 0.95, 0.99)
		if v, ok := pcts[""]; ok && len(v) == 2 {
			if v[0] > 0 {
				ms := v[0] * 1000.0
				p95Ms = &ms
			}
			if v[1] > 0 {
				ms := v[1] * 1000.0
				p99Ms = &ms
			}
		}
	}

	var errorRatePtr *float64
	if main.TotalCount > 0 {
		rate := float64(main.ErrorCount) / durationSecs
		errorRatePtr = &rate
	}

	conn, _ := s.repo.GetSummaryConn(ctx, teamID, startMs, endMs, f)
	var activeConns int64
	if conn.UsedCount > 0 {
		activeConns = int64(conn.UsedSum / float64(conn.UsedCount))
	}

	var cacheHitRate *float64
	if cache, err := s.repo.GetSummaryCache(ctx, teamID, startMs, endMs, f); err == nil && cache.TotalCount > 0 {
		rate := float64(cache.SuccessCount) / float64(cache.TotalCount) * 100
		cacheHitRate = &rate
	}

	return SummaryStats{
		AvgLatencyMs:      avgLatencyMs,
		P95LatencyMs:      p95Ms,
		P99LatencyMs:      p99Ms,
		SpanCount:         int64(main.TotalCount), //nolint:gosec // tenant-scoped span count fits int64
		ActiveConnections: activeConns,
		ErrorRate:         errorRatePtr,
		CacheHitRate:      cacheHitRate,
	}, nil
}
