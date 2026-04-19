package sketch

import (
	"context"
	"errors"
	"log/slog"
)

// Querier is the service-layer entry point for reading sketches. Repositories
// fetch primitive counts from ClickHouse; services call Querier to attach
// percentiles / cardinality without any heavy SQL.
type Querier struct {
	store    Store
	fallback Fallback
}

// NewQuerier wires a Store and an optional CH fallback. Fallback may be nil
// in tests; in production it should always be set so sketch misses don't
// silently zero out results.
func NewQuerier(store Store, fallback Fallback) *Querier {
	return &Querier{store: store, fallback: fallback}
}

// Percentiles returns {dim → [q0, q1, ...]} for the given kind + range.
func (q *Querier) Percentiles(ctx context.Context, kind Kind, teamID string, startMs, endMs int64, qs ...float64) (map[string][]float64, error) {
	if q == nil || q.store == nil {
		return nil, errors.New("sketch: querier not configured")
	}
	if kind.Family != FamilyDistribution {
		return nil, errors.New("sketch: kind is not a distribution")
	}
	groups, err := q.store.LoadDigests(ctx, kind, teamID, startMs, endMs)
	if err != nil {
		slog.Warn("sketch: load failed, using fallback", slog.String("kind", kind.ID), slog.Any("error", err))
		if q.fallback == nil {
			return nil, err
		}
		return q.fallback.Quantiles(ctx, kind, teamID, startMs, endMs, qs...)
	}
	out := make(map[string][]float64, len(groups))
	for dim, ds := range groups {
		merged := NewDigest()
		for _, d := range ds {
			_ = merged.Merge(d)
		}
		out[dim] = merged.Quantiles(qs...)
	}
	return out, nil
}

// Cardinality returns {dim → estimated uniques} for the given kind + range.
func (q *Querier) Cardinality(ctx context.Context, kind Kind, teamID string, startMs, endMs int64) (map[string]uint64, error) {
	if q == nil || q.store == nil {
		return nil, errors.New("sketch: querier not configured")
	}
	if kind.Family != FamilyCardinality {
		return nil, errors.New("sketch: kind is not a cardinality sketch")
	}
	groups, err := q.store.LoadHLLs(ctx, kind, teamID, startMs, endMs)
	if err != nil {
		slog.Warn("sketch: load failed, using fallback", slog.String("kind", kind.ID), slog.Any("error", err))
		if q.fallback == nil {
			return nil, err
		}
		return q.fallback.Uniques(ctx, kind, teamID, startMs, endMs)
	}
	out := make(map[string]uint64, len(groups))
	for dim, hs := range groups {
		merged := NewHLL()
		for _, h := range hs {
			_ = merged.Merge(h)
		}
		out[dim] = merged.Estimate()
	}
	return out, nil
}

// ZipPercentiles is a convenience helper: given rows carrying a Dim key and
// mutable Percentiles slots, it fills the percentile slots from the sketches.
// Rows with no sketch coverage stay untouched (callers may choose to null
// those percentiles or trigger a narrower fallback).
type RowWithPercentiles interface {
	// Dim is the stable dimension string that was used on ingest.
	Dim() string
	// SetPercentiles receives values in the same order as qs passed to ZipPercentiles.
	SetPercentiles(vals []float64)
}

func (q *Querier) ZipPercentiles(ctx context.Context, rows []RowWithPercentiles, kind Kind, teamID string, startMs, endMs int64, qs ...float64) error {
	if len(rows) == 0 {
		return nil
	}
	pcts, err := q.Percentiles(ctx, kind, teamID, startMs, endMs, qs...)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if v, ok := pcts[row.Dim()]; ok {
			row.SetPercentiles(v)
		}
	}
	return nil
}
