package slowqueries

import (
	"context"
	"fmt"
	"sort"
	"strings"

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

// GetSlowQueryPatterns — percentiles come from DbQueryLatency, keyed by
// system|queryTextFingerprint. Rows missing a fingerprint stay at zero
// (ingest-side consumer only observes DbQueryLatency when fingerprint is
// non-empty, so the miss is coherent with the sketch coverage).
func (s *Service) GetSlowQueryPatterns(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]SlowQueryPattern, error) {
	rows, err := s.repo.GetSlowQueryPatterns(ctx, teamID, startMs, endMs, f, limit)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.DbQueryLatency, teamIDString(teamID), startMs, endMs, 0.50, 0.95, 0.99)
		for i := range rows {
			if rows[i].QueryTextFingerprint == "" {
				continue
			}
			dim := sketch.DimDbQuery(rows[i].DBSystem, rows[i].QueryTextFingerprint)
			if v, ok := pcts[dim]; ok && len(v) == 3 {
				p50 := v[0] * 1000.0
				p95 := v[1] * 1000.0
				p99 := v[2] * 1000.0
				rows[i].P50Ms = &p50
				rows[i].P95Ms = &p95
				rows[i].P99Ms = &p99
			}
		}
	}
	// Restore the original ORDER BY p99_ms DESC in Go after percentile fill.
	sort.SliceStable(rows, func(a, b int) bool {
		av := float64(0)
		bv := float64(0)
		if rows[a].P99Ms != nil {
			av = *rows[a].P99Ms
		}
		if rows[b].P99Ms != nil {
			bv = *rows[b].P99Ms
		}
		return av > bv
	})
	return rows, nil
}

func (s *Service) GetSlowestCollections(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]SlowCollectionRow, error) {
	rows, err := s.repo.GetSlowestCollections(ctx, teamID, startMs, endMs, f)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		// Aggregate per-collection p99 by scanning the DbOpLatency dim map
		// (system|op|coll|ns) and taking the max per-op p99 per collection.
		// Cheap at the ~50-row limit this endpoint enforces.
		all, err := s.sketchQ.Percentiles(ctx, sketch.DbOpLatency, teamIDString(teamID), startMs, endMs, 0.99)
		if err == nil {
			for i, r := range rows {
				collMid := "|" + r.CollectionName + "|"
				sysPrefix := r.DBSystem + "|"
				var best float64
				found := false
				for dim, v := range all {
					if !strings.HasPrefix(dim, sysPrefix) {
						continue
					}
					if !strings.Contains(dim, collMid) {
						continue
					}
					if len(v) == 0 {
						continue
					}
					if !found || v[0] > best {
						best = v[0]
						found = true
					}
				}
				if found {
					ms := best * 1000.0
					rows[i].P99Ms = &ms
				}
			}
		}
	}
	// Re-sort by p99 DESC (matches the original SQL intent now that the
	// percentile is Go-filled).
	sort.SliceStable(rows, func(a, b int) bool {
		av := float64(0)
		bv := float64(0)
		if rows[a].P99Ms != nil {
			av = *rows[a].P99Ms
		}
		if rows[b].P99Ms != nil {
			bv = *rows[b].P99Ms
		}
		return av > bv
	})
	return rows, nil
}

func (s *Service) GetSlowQueryRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	return s.repo.GetSlowQueryRate(ctx, teamID, startMs, endMs, f, thresholdMs)
}

func (s *Service) GetP99ByQueryText(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters, limit int) ([]P99ByQueryText, error) {
	rows, err := s.repo.GetP99ByQueryText(ctx, teamID, startMs, endMs, f, limit)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.DbQueryLatency, teamIDString(teamID), startMs, endMs, 0.99)
		for i := range rows {
			if rows[i].QueryTextFingerprint == "" {
				continue
			}
			dim := sketch.DimDbQuery(rows[i].DBSystem, rows[i].QueryTextFingerprint)
			if v, ok := pcts[dim]; ok && len(v) == 1 {
				ms := v[0] * 1000.0
				rows[i].P99Ms = &ms
			}
		}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		av := float64(0)
		bv := float64(0)
		if rows[a].P99Ms != nil {
			av = *rows[a].P99Ms
		}
		if rows[b].P99Ms != nil {
			bv = *rows[b].P99Ms
		}
		return av > bv
	})
	return rows, nil
}
