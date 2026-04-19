package sketch

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Aggregator holds in-process rolling sketches per (kind, bucket, dim).
// One instance per ingest consumer — callers push every record through
// Observe*; a single-goroutine flush ticker drains closed buckets to Redis.
type Aggregator struct {
	mu            sync.Mutex
	digests       map[aggKey]*Digest
	hlls          map[aggKey]*HLL
	dimLabels     map[aggKey]string
	maxActiveDims int
	evictions     uint64
}

type aggKey struct {
	kindID   string
	teamID   string
	dimHash  string
	bucketTs int64 // unix seconds, bucket-aligned
}

// NewAggregator returns an empty aggregator. maxActiveDims caps the number of
// live sketches across all kinds — overflow evicts the first-encountered dim
// and increments an internal counter (exposed via Evictions() for metrics).
func NewAggregator(maxActiveDims int) *Aggregator {
	if maxActiveDims <= 0 {
		maxActiveDims = 50_000
	}
	return &Aggregator{
		digests:       make(map[aggKey]*Digest),
		hlls:          make(map[aggKey]*HLL),
		dimLabels:     make(map[aggKey]string),
		maxActiveDims: maxActiveDims,
	}
}

// ObserveLatency records a distribution sample for (kind, teamID, dim) at the
// bucket containing tsUnix.
func (a *Aggregator) ObserveLatency(kind Kind, teamID, dim string, value float64, weight uint32, tsUnix int64) {
	if kind.Family != FamilyDistribution || teamID == "" || dim == "" {
		return
	}
	key := a.keyFor(kind, teamID, dim, tsUnix)
	a.mu.Lock()
	defer a.mu.Unlock()
	d, ok := a.digests[key]
	if !ok {
		if a.exceedsCap() {
			a.evictions++
			return
		}
		d = NewDigest()
		a.digests[key] = d
		a.dimLabels[key] = dim
	}
	if err := d.Add(value, weight); err != nil {
		slog.Debug("sketch: add latency failed", slog.String("kind", kind.ID), slog.Any("error", err))
	}
}

// ObserveIdentity records an element for a cardinality sketch.
func (a *Aggregator) ObserveIdentity(kind Kind, teamID, dim, element string, tsUnix int64) {
	if kind.Family != FamilyCardinality || teamID == "" || dim == "" || element == "" {
		return
	}
	key := a.keyFor(kind, teamID, dim, tsUnix)
	a.mu.Lock()
	defer a.mu.Unlock()
	h, ok := a.hlls[key]
	if !ok {
		if a.exceedsCap() {
			a.evictions++
			return
		}
		h = NewHLL()
		a.hlls[key] = h
		a.dimLabels[key] = dim
	}
	h.AddString(element)
}

func (a *Aggregator) keyFor(kind Kind, teamID, dim string, tsUnix int64) aggKey {
	ts := time.Unix(tsUnix, 0).UTC().Truncate(kind.Bucket).Unix()
	return aggKey{
		kindID:   kind.ID,
		teamID:   teamID,
		dimHash:  HashDim(dim),
		bucketTs: ts,
	}
}

func (a *Aggregator) exceedsCap() bool {
	return len(a.digests)+len(a.hlls) >= a.maxActiveDims
}

// FlushClosed writes every bucket whose end (bucketTs + kind.Bucket) is
// before (now - grace) and drops them from memory. Current-minute sketches
// stay in memory until their next flush tick.
func (a *Aggregator) FlushClosed(ctx context.Context, now time.Time, store Store, grace time.Duration) error {
	cutoff := now.Add(-grace).UTC().Unix()

	a.mu.Lock()
	toWriteDigests := make(map[aggKey]*Digest)
	toWriteHLLs := make(map[aggKey]*HLL)
	toWriteDims := make(map[aggKey]string)
	for k, d := range a.digests {
		if closed := k.bucketTs + int64(bucketForID(k.kindID).Seconds()); closed <= cutoff {
			toWriteDigests[k] = d
			toWriteDims[k] = a.dimLabels[k]
			delete(a.digests, k)
			delete(a.dimLabels, k)
		}
	}
	for k, h := range a.hlls {
		if closed := k.bucketTs + int64(bucketForID(k.kindID).Seconds()); closed <= cutoff {
			toWriteHLLs[k] = h
			toWriteDims[k] = a.dimLabels[k]
			delete(a.hlls, k)
			delete(a.dimLabels, k)
		}
	}
	a.mu.Unlock()

	for k, d := range toWriteDigests {
		kind, ok := kindByID(k.kindID)
		if !ok {
			continue
		}
		key := Key{
			Kind:     kind,
			TeamID:   k.teamID,
			DimHash:  k.dimHash,
			BucketTs: time.Unix(k.bucketTs, 0).UTC(),
		}
		if err := store.WriteDigest(ctx, key, d, toWriteDims[k]); err != nil {
			slog.Warn("sketch: flush digest failed", slog.String("kind", kind.ID), slog.Any("error", err))
		}
	}
	for k, h := range toWriteHLLs {
		kind, ok := kindByID(k.kindID)
		if !ok {
			continue
		}
		key := Key{
			Kind:     kind,
			TeamID:   k.teamID,
			DimHash:  k.dimHash,
			BucketTs: time.Unix(k.bucketTs, 0).UTC(),
		}
		if err := store.WriteHLL(ctx, key, h, toWriteDims[k]); err != nil {
			slog.Warn("sketch: flush hll failed", slog.String("kind", kind.ID), slog.Any("error", err))
		}
	}
	return nil
}

// Evictions returns the cumulative count of observations dropped because the
// active-dim cap was hit. Useful as a metric.
func (a *Aggregator) Evictions() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.evictions
}

func kindByID(id string) (Kind, bool) {
	for _, k := range AllKinds {
		if k.ID == id {
			return k, true
		}
	}
	return Kind{}, false
}

func bucketForID(id string) time.Duration {
	if k, ok := kindByID(id); ok {
		return k.Bucket
	}
	return time.Minute
}
