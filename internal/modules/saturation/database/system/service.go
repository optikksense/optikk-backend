package system

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

// GetSystemLatency — per-operation percentile attach is a known limitation
// (operation sits at position 2 of the DbOpLatency dim tuple and
// PercentilesByDimPrefix only prefixes). We emit the system-wide aggregate
// on every row instead so the chart still carries a p50/p95/p99 band.
func (s *Service) GetSystemLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetSystemLatency(ctx, teamID, startMs, endMs, dbSystem, f)
	if err != nil {
		return nil, err
	}
	if s.sketchQ != nil && len(rows) > 0 {
		pcts, _ := s.sketchQ.PercentilesByDimPrefix(ctx, sketch.DbOpLatency, teamIDString(teamID), startMs, endMs, []string{dbSystem + "|"}, 0.50, 0.95, 0.99)
		if v, ok := pcts[dbSystem+"|"]; ok && len(v) == 3 {
			p50 := v[0] * 1000.0
			p95 := v[1] * 1000.0
			p99 := v[2] * 1000.0
			for i := range rows {
				rows[i].P50Ms = &p50
				rows[i].P95Ms = &p95
				rows[i].P99Ms = &p99
			}
		}
	}
	return rows, nil
}

func (s *Service) GetSystemOps(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetSystemOps(ctx, teamID, startMs, endMs, dbSystem, f)
}

// fillCollectionP99 attaches p99 per collection by loading all DbOpLatency
// dims for the tenant and filtering to (system=dbSystem, collection=row.C).
// Dim tuple position 3 is collection; scanning is O(tenant-dims) which is
// fine at small tenant sizes and amortized across collections.
func (s *Service) fillCollectionP99(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, rows []SystemCollectionRow) {
	if s.sketchQ == nil || len(rows) == 0 {
		return
	}
	all, err := s.sketchQ.Percentiles(ctx, sketch.DbOpLatency, teamIDString(teamID), startMs, endMs, 0.99)
	if err != nil {
		return
	}
	// Bucket dims by system+collection prefix. dims are system|op|coll|ns.
	// We don't re-merge sketches here — Percentiles already returns the
	// per-dim p99; within a collection we take the max across ops as a
	// conservative tail estimate (p99 of a union ≥ max per-group p99).
	for i, r := range rows {
		var best float64
		found := false
		sysPrefix := dbSystem + "|"
		collMid := "|" + r.CollectionName + "|"
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

func (s *Service) GetSystemTopCollectionsByLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	rows, err := s.repo.GetSystemTopCollectionsByLatency(ctx, teamID, startMs, endMs, dbSystem)
	if err != nil {
		return nil, err
	}
	s.fillCollectionP99(ctx, teamID, startMs, endMs, dbSystem, rows)
	// Re-sort by p99 DESC (fallback to sum-avg-ms) so the "by latency"
	// endpoint actually sorts by the sketch-derived p99 we just filled.
	sort.SliceStable(rows, func(a, b int) bool {
		av := float64(0)
		bv := float64(0)
		if rows[a].P99Ms != nil {
			av = *rows[a].P99Ms
		}
		if rows[b].P99Ms != nil {
			bv = *rows[b].P99Ms
		}
		if av == bv {
			return avgMs(rows[a]) > avgMs(rows[b])
		}
		return av > bv
	})
	return rows, nil
}

func avgMs(r SystemCollectionRow) float64 {
	if r.LatencyCount == 0 {
		return 0
	}
	return (r.LatencySum / float64(r.LatencyCount)) * 1000.0
}

func (s *Service) GetSystemTopCollectionsByVolume(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	rows, err := s.repo.GetSystemTopCollectionsByVolume(ctx, teamID, startMs, endMs, dbSystem)
	if err != nil {
		return nil, err
	}
	s.fillCollectionP99(ctx, teamID, startMs, endMs, dbSystem, rows)
	return rows, nil
}

func (s *Service) GetSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	return s.repo.GetSystemErrors(ctx, teamID, startMs, endMs, dbSystem)
}

func (s *Service) GetSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	return s.repo.GetSystemNamespaces(ctx, teamID, startMs, endMs, dbSystem)
}
