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
			if d == nil {
				continue
			}
			_ = merged.MergeWith(d)
		}
		if merged.IsEmpty() {
			out[dim] = make([]float64, len(qs))
			continue
		}
		vals, err := merged.GetValuesAtQuantiles(qs)
		if err != nil {
			slog.Debug("sketch: quantile compute failed", slog.String("kind", kind.ID), slog.Any("error", err))
			out[dim] = make([]float64, len(qs))
			continue
		}
		out[dim] = vals
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

// PercentilesByDimPrefix collapses every dim whose string starts with prefix
// into a single merged sketch. Used by saturation sub-modules that read at
// a coarser granularity than the ingest-side dim tuple — e.g. the `systems`
// view wants one result per db_system, but DbOpLatency keys by
// system|operation|collection|namespace. Service passes `system+"|"` and
// gets the merged percentiles.
func (q *Querier) PercentilesByDimPrefix(ctx context.Context, kind Kind, teamID string, startMs, endMs int64, prefixes []string, qs ...float64) (map[string][]float64, error) {
	if q == nil || q.store == nil {
		return nil, errors.New("sketch: querier not configured")
	}
	if kind.Family != FamilyDistribution {
		return nil, errors.New("sketch: kind is not a distribution")
	}
	all, err := q.store.LoadDigests(ctx, kind, teamID, startMs, endMs)
	if err != nil {
		if q.fallback == nil {
			return nil, err
		}
		return q.fallback.Quantiles(ctx, kind, teamID, startMs, endMs, qs...)
	}
	out := make(map[string][]float64, len(prefixes))
	for _, prefix := range prefixes {
		merged := NewDigest()
		seen := false
		for dim, ds := range all {
			if !startsWith(dim, prefix) {
				continue
			}
			for _, d := range ds {
				if d == nil {
					continue
				}
				if err := merged.MergeWith(d); err == nil {
					seen = true
				}
			}
		}
		if !seen || merged.IsEmpty() {
			out[prefix] = make([]float64, len(qs))
			continue
		}
		vals, qErr := merged.GetValuesAtQuantiles(qs)
		if qErr != nil {
			out[prefix] = make([]float64, len(qs))
			continue
		}
		out[prefix] = vals
	}
	return out, nil
}

// CountRow is the minimal surface ZipCounts needs: a dim string + slots for
// the percentile values it'll receive. Implementations are usually anonymous
// structs in service.go.
type CountRow interface {
	Dim() string
	SetPercentiles(vals []float64)
}

// ZipCounts fills the percentile slots of every row from the map produced
// by Percentiles / PercentilesByDimPrefix. Returns the number of rows that
// matched a sketch — useful for observability (low match rate → cold
// sketches / wrong dim string / stale tenant).
func (q *Querier) ZipCounts(rows []CountRow, pcts map[string][]float64) int {
	matched := 0
	for _, row := range rows {
		if v, ok := pcts[row.Dim()]; ok {
			row.SetPercentiles(v)
			matched++
		}
	}
	return matched
}

func startsWith(s, prefix string) bool {
	if len(prefix) == 0 {
		return true
	}
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// TimeseriesPoint is one (bucketUnix, quantile-values) entry per (dim, step).
type TimeseriesPoint struct {
	BucketTs int64
	Values   []float64 // aligned with qs passed to PercentilesTimeseries
}

// PercentilesTimeseries returns {dim → ordered []TimeseriesPoint} with
// stepSecs-aligned buckets. Multiple ingest-bucket sketches inside the same
// step window are merged into one sketch before quantile computation.
// If stepSecs == 0 or < kind.Bucket, the kind's native bucket is used.
// Falls through to CHFallback as a single aggregate (no time-series) when
// sketch load fails — correct, slightly degraded on that path.
func (q *Querier) PercentilesTimeseries(ctx context.Context, kind Kind, teamID string, startMs, endMs, stepSecs int64, qs ...float64) (map[string][]TimeseriesPoint, error) {
	if q == nil || q.store == nil {
		return nil, errors.New("sketch: querier not configured")
	}
	if kind.Family != FamilyDistribution {
		return nil, errors.New("sketch: kind is not a distribution")
	}
	step := stepSecs
	kindBucket := int64(kind.Bucket.Seconds())
	if step < kindBucket {
		step = kindBucket
	}
	bucketed, err := q.store.LoadDigestsBucketed(ctx, kind, teamID, startMs, endMs)
	if err != nil {
		slog.Warn("sketch: timeseries load failed", slog.String("kind", kind.ID), slog.Any("error", err))
		return map[string][]TimeseriesPoint{}, nil
	}
	out := make(map[string][]TimeseriesPoint, len(bucketed))
	for dim, byBucket := range bucketed {
		stepGroup := make(map[int64]*Digest, len(byBucket))
		for ts, d := range byBucket {
			if d == nil {
				continue
			}
			floor := (ts / step) * step
			existing, ok := stepGroup[floor]
			if !ok {
				stepGroup[floor] = d
				continue
			}
			_ = existing.MergeWith(d)
		}
		points := make([]TimeseriesPoint, 0, len(stepGroup))
		for ts, merged := range stepGroup {
			if merged == nil || merged.IsEmpty() {
				continue
			}
			vals, qErr := merged.GetValuesAtQuantiles(qs)
			if qErr != nil {
				continue
			}
			points = append(points, TimeseriesPoint{BucketTs: ts, Values: vals})
		}
		sortPointsByTs(points)
		out[dim] = points
	}
	return out, nil
}

func sortPointsByTs(points []TimeseriesPoint) {
	// Small N per dim; insertion sort is fine and keeps this dependency-free.
	for i := 1; i < len(points); i++ {
		for j := i; j > 0 && points[j-1].BucketTs > points[j].BucketTs; j-- {
			points[j-1], points[j] = points[j], points[j-1]
		}
	}
}
