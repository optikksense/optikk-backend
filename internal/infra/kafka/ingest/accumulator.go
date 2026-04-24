// Package ingest holds the generic ingest-pipeline building blocks shared
// between logs, spans, and metrics. A signal's consumer composes: Dispatcher
// (one PollFetches loop) → per-partition Worker (one goroutine) → Accumulator
// (size + time trigger) → Writer (CH batch send + retry + DLQ).
//
// Generics let all signals reuse the same goroutine and timer machinery; the
// only per-signal code is an `Item` shape (payload + raw Kafka record) and
// the writer's CH insert closure.
package ingest

import (
	"time"
)

// FlushReason describes why an Accumulator handed a batch to its caller.
// The string form doubles as the `reason` label on
// `optikk_ingest_worker_flush_rows`.
type FlushReason int

const (
	FlushSize  FlushReason = iota // MaxRows reached
	FlushBytes                    // MaxBytes reached
	FlushTime                     // MaxAge elapsed since first item
	FlushStop                     // Shutdown drain
)

// String returns the label value used on Prometheus series. Kept short and
// low-cardinality.
func (r FlushReason) String() string {
	switch r {
	case FlushSize:
		return "size"
	case FlushBytes:
		return "bytes"
	case FlushTime:
		return "time"
	case FlushStop:
		return "stop"
	default:
		return "unknown"
	}
}

// AccumulatorConfig bounds a single accumulator. The zero value is not usable;
// prefer NewAccumulator with a config derived from config.IngestPipeline.
type AccumulatorConfig struct {
	MaxRows  int
	MaxBytes int
	MaxAge   time.Duration
}

// DefaultAccumulatorConfig returns the plan-mandated triggers tuned for the
// 200k records/s/instance target: 10k rows OR 16 MiB OR 250 ms.
func DefaultAccumulatorConfig() AccumulatorConfig {
	return AccumulatorConfig{
		MaxRows:  10_000,
		MaxBytes: 16 * 1024 * 1024,
		MaxAge:   250 * time.Millisecond,
	}
}

// Accumulator collects one partition's items and hands them off in batches
// whenever any trigger fires. It is NOT goroutine-safe; use one per worker.
type Accumulator[T any] struct {
	cfg       AccumulatorConfig
	items     []T
	bytes     int
	lastBytes int
	first     time.Time
	sizeFn    func(T) int
}

// NewAccumulator takes the size function used to account for the bytes trigger
// (usually len(record.Value) or similar).
func NewAccumulator[T any](cfg AccumulatorConfig, sizeFn func(T) int) *Accumulator[T] {
	if cfg.MaxRows <= 0 {
		cfg = DefaultAccumulatorConfig()
	}
	return &Accumulator[T]{cfg: cfg, sizeFn: sizeFn}
}

// Add appends one item. Returns (batch, reason, true) when a trigger fired
// and the caller should flush now; otherwise (nil, 0, false).
func (a *Accumulator[T]) Add(item T) ([]T, FlushReason, bool) {
	if len(a.items) == 0 {
		a.first = time.Now()
	}
	a.items = append(a.items, item)
	if a.sizeFn != nil {
		a.bytes += a.sizeFn(item)
	}
	switch {
	case len(a.items) >= a.cfg.MaxRows:
		return a.drain(), FlushSize, true
	case a.bytes >= a.cfg.MaxBytes:
		return a.drain(), FlushBytes, true
	}
	return nil, 0, false
}

// Due reports whether the age trigger has fired since the first-added item.
// Called from a ticker in the worker; returns (batch, FlushTime, true) when
// due. The reason is always FlushTime — returned alongside the batch so the
// caller can feed the flush_rows metric without a separate enum check.
func (a *Accumulator[T]) Due(now time.Time) ([]T, FlushReason, bool) {
	if len(a.items) == 0 {
		return nil, 0, false
	}
	if now.Sub(a.first) < a.cfg.MaxAge {
		return nil, 0, false
	}
	return a.drain(), FlushTime, true
}

// DrainAll empties the accumulator unconditionally — used on Stop().
func (a *Accumulator[T]) DrainAll() []T {
	if len(a.items) == 0 {
		return nil
	}
	return a.drain()
}

// BytesAtFlush returns the byte-count that triggered the most recent drain,
// intended for the `worker_flush_bytes` histogram. Read this inside the
// same goroutine as the drain — it is reset each drain.
func (a *Accumulator[T]) BytesAtFlush() int { return a.lastBytes }

func (a *Accumulator[T]) drain() []T {
	out := a.items
	a.lastBytes = a.bytes
	a.items = nil
	a.bytes = 0
	a.first = time.Time{}
	return out
}
