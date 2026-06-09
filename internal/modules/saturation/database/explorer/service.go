package explorer

import (
	"context"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

// GetDatastoreSystems fetches datastore summaries and connections.
func (s *Service) GetDatastoreSystems(ctx context.Context, teamID, startMs, endMs int64) ([]DatastoreSystemRow, error) {
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
		// Active-connection metric is best-effort; absence shouldn't fail the query.
		c, err := s.repo.GetActiveConnectionsBySystem(gctx, teamID, startMs, endMs)
		if err == nil {
			conns = c
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	rows := make([]DatastoreSystemRow, 0, len(spanRows))
	for _, r := range spanRows {
		queryCount := int64(r.QueryCount)
		errorCount := int64(r.ErrorCount)
		rows = append(rows, DatastoreSystemRow{
			System:            r.DBSystem,
			Category:          datastoreCategory(r.DBSystem),
			QueryCount:        queryCount,
			AvgLatencyMs:      r.AvgLatencyMs,
			P95LatencyMs:      float64(r.P95Ms),
			ErrorRate:         safeRatioPct(errorCount, queryCount),
			ActiveConnections: conns[r.DBSystem],
			ServerHint:        r.ServerAddress,
			LastSeen:          r.LastSeen.Format(time.RFC3339),
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].QueryCount == rows[j].QueryCount {
			return rows[i].System < rows[j].System
		}
		return rows[i].QueryCount > rows[j].QueryCount
	})

	return rows, nil
}

func datastoreCategory(system string) string {
	if strings.EqualFold(strings.TrimSpace(system), "redis") {
		return "redis"
	}
	return "database"
}

func safeRatioPct(numerator int64, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator) * 100
}
