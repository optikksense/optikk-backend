package summary

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

// GetSummaryStats fans out main / connections / cache queries in parallel
// and assembles the aggregate response. Latency p95/p99 interpolated
// Go-side from the bucket histogram.
func (s *Service) GetSummaryStats(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) (SummaryStats, error) {
	var (
		main    mainRawRow
		mainOK  bool
		conns   int64
		cache   cacheRawRow
		cacheOK bool
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		row, err := s.repo.GetMainStats(gctx, teamID, startMs, endMs, f)
		if err != nil {
			return err
		}
		main, mainOK = row, true
		return nil
	})
	g.Go(func() error {
		c, _ := s.repo.GetActiveConnections(gctx, teamID, startMs, endMs)
		conns = c
		return nil
	})
	g.Go(func() error {
		row, err := s.repo.GetCacheStats(gctx, teamID, startMs, endMs, f)
		if err == nil {
			cache, cacheOK = row, true
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return SummaryStats{}, err
	}

	out := SummaryStats{ActiveConnections: conns}
	if mainOK {
		avg := main.AvgMs
		p95 := main.P95Ms
		p99 := main.P99Ms
		out.AvgLatencyMs = &avg
		out.P95LatencyMs = &p95
		out.P99LatencyMs = &p99
		out.SpanCount = int64(main.TotalCount) //nolint:gosec // domain-bounded
		if main.TotalCount > 0 {
			rate := float64(main.ErrorCount) / float64(main.TotalCount) * 100.0
			out.ErrorRate = &rate
		}
	}
	if cacheOK && cache.TotalCount > 0 {
		hit := float64(cache.SuccessCount) / float64(cache.TotalCount) * 100.0
		out.CacheHitRate = &hit
	}
	return out, nil
}
