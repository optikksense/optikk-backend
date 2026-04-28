package systems

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
	"github.com/Optikk-Org/optikk-backend/internal/shared/quantile"
	"golang.org/x/sync/errgroup"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetDetectedSystems(ctx context.Context, teamID, startMs, endMs int64) ([]DetectedSystem, error) {
	rows, err := s.repo.GetDetectedSystems(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]DetectedSystem, len(rows))
	for i, r := range rows {
		out[i] = DetectedSystem{
			DBSystem:      r.DBSystem,
			SpanCount:     r.SpanCount,
			ErrorCount:    r.ErrorCount,
			AvgLatencyMs:  r.AvgLatencyMs,
			QueryCount:    r.SpanCount, // alias — every span is a query
			ServerAddress: r.ServerAddress,
			LastSeen:      r.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}

// GetSystemSummaries fans out the spans-side aggregate + the metrics-side
// active-connections query in parallel, interpolates p95 from the bucket
// histogram, and joins on db_system.
func (s *Service) GetSystemSummaries(ctx context.Context, teamID, startMs, endMs int64) ([]SystemSummary, error) {
	var (
		spanRows []systemSummaryRawDTO
		conns    map[string]int64
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rows, err := s.repo.GetSystemSummariesRaw(gctx, teamID, startMs, endMs)
		if err != nil {
			return err
		}
		spanRows = rows
		return nil
	})
	g.Go(func() error {
		// Active-connection metric is best-effort; absence shouldn't fail
		// the whole panel.
		c, err := s.repo.GetActiveConnectionsBySystem(gctx, teamID, startMs, endMs)
		if err == nil {
			conns = c
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	out := make([]SystemSummary, len(spanRows))
	for i, r := range spanRows {
		p95 := quantile.FromHistogram(filter.LatencyBucketBoundsMs, r.Buckets, 0.95)
		out[i] = SystemSummary{
			DBSystem:          r.DBSystem,
			QueryCount:        r.QueryCount,
			ErrorCount:        r.ErrorCount,
			AvgLatencyMs:      r.AvgLatencyMs,
			P95LatencyMs:      p95,
			ActiveConnections: conns[r.DBSystem],
			ServerAddress:     r.ServerAddress,
			LastSeen:          r.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}
