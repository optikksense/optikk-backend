package collection

import (
	"context"
	"fmt"
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

// GetCollectionLatency attaches p50/p95/p99 per (time_bucket, operation) by
// loading the DbOpLatency dim map and merging across all operations that
// match (system=any, collection=row.C). Since operation sits at position 2
// and collection at position 3 of the dim tuple, we scan and filter in Go.
// We attach the same across-ops aggregate to every time_bucket for a given
// operation (known limitation — per-bucket percentiles would need a second
// sketch kind).
func (s *Service) GetCollectionLatency(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]LatencyTimeSeries, error) {
	rows, err := s.repo.GetCollectionLatency(ctx, teamID, startMs, endMs, collection, f)
	if err != nil {
		return nil, err
	}
	if s.sketchQ == nil || len(rows) == 0 {
		return rows, nil
	}
	all, err := s.sketchQ.Percentiles(ctx, sketch.DbOpLatency, teamIDString(teamID), startMs, endMs, 0.50, 0.95, 0.99)
	if err != nil || len(all) == 0 {
		return rows, nil
	}
	// Bucket by operation (the row's group_by). Collection must appear at
	// position 3 of the dim tuple.
	collMid := "|" + collection + "|"
	byOp := make(map[string][3]float64, 16)
	for dim, v := range all {
		if len(v) != 3 {
			continue
		}
		if !strings.Contains(dim, collMid) {
			continue
		}
		parts := strings.Split(dim, "|")
		if len(parts) < 2 {
			continue
		}
		op := parts[1]
		cur, ok := byOp[op]
		if !ok {
			byOp[op] = [3]float64{v[0], v[1], v[2]}
			continue
		}
		// Max-per-percentile is a safe union-bound approximation without
		// re-merging sketches in Go.
		cur[0] = max64(cur[0], v[0])
		cur[1] = max64(cur[1], v[1])
		cur[2] = max64(cur[2], v[2])
		byOp[op] = cur
	}
	for i := range rows {
		if vals, ok := byOp[rows[i].GroupBy]; ok {
			p50 := vals[0] * 1000.0
			p95 := vals[1] * 1000.0
			p99 := vals[2] * 1000.0
			rows[i].P50Ms = &p50
			rows[i].P95Ms = &p95
			rows[i].P99Ms = &p99
		}
	}
	return rows, nil
}

func max64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func (s *Service) GetCollectionOps(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]OpsTimeSeries, error) {
	return s.repo.GetCollectionOps(ctx, teamID, startMs, endMs, collection, f)
}

func (s *Service) GetCollectionErrors(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters) ([]ErrorTimeSeries, error) {
	return s.repo.GetCollectionErrors(ctx, teamID, startMs, endMs, collection, f)
}

func (s *Service) GetCollectionQueryTexts(ctx context.Context, teamID int64, startMs, endMs int64, collection string, f shared.Filters, limit int) ([]CollectionTopQuery, error) {
	rows, err := s.repo.GetCollectionQueryTexts(ctx, teamID, startMs, endMs, collection, f, limit)
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
	return rows, nil
}

func (s *Service) GetCollectionReadVsWrite(ctx context.Context, teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error) {
	return s.repo.GetCollectionReadVsWrite(ctx, teamID, startMs, endMs, collection)
}
