// Package ingest holds the generic ingest-pipeline building blocks shared
// between logs and spans. A signal's consumer composes: Dispatcher (one
// PollFetches loop) → per-partition Worker (one goroutine) → Accumulator
// (size + time trigger) → Writer (CH batch send + retry + DLQ).
//
// Generics let both signals reuse the same goroutine and timer machinery; the
// only per-signal code is an `Item` shape (payload + raw Kafka record) and
// the writer's CH insert closure.
package ingest

import (
	"time"
)

// FlushReason describes why an Accumulator handed a batch to its caller.
type FlushReason int

const (
	FlushSize  FlushReason = iota // 5000 rows
	FlushBytes                    // 8 MiB
	FlushTime                     // 500 ms tick
	FlushStop                     // Stop() called
)

// AccumulatorConfig bounds a single accumulator. The zero value is not usable.
type AccumulatorConfig struct {
	MaxRows     int
	MaxBytes    int
	MaxAge      time.Duration
}

// DefaultAccumulatorConfig returns the plan-mandated triggers: 5000 rows OR
// 8 MiB OR 500 ms.
func DefaultAccumulatorConfig() AccumulatorConfig {
	return AccumulatorConfig{
		MaxRows:  5000,
		MaxBytes: 8 * 1024 * 1024,
		MaxAge:   500 * time.Millisecond,
	}
}

// Accumulator collects one partition's items and hands them off in batches
// whenever any trigger fires. It is NOT goroutine-safe; use one per worker.
type Accumulator[T any] struct {
	cfg    AccumulatorConfig
	items  []T
	bytes  int
	first  time.Time
	sizeFn func(T) int
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
// Called from a ticker in the worker; returns (batch, true) when due.
func (a *Accumulator[T]) Due(now time.Time) ([]T, bool) {
	if len(a.items) == 0 {
		return nil, false
	}
	if now.Sub(a.first) < a.cfg.MaxAge {
		return nil, false
	}
	return a.drain(), true
}

// DrainAll empties the accumulator unconditionally — used on Stop().
func (a *Accumulator[T]) DrainAll() []T {
	if len(a.items) == 0 {
		return nil
	}
	return a.drain()
}

func (a *Accumulator[T]) drain() []T {
	out := a.items
	a.items = nil
	a.bytes = 0
	a.first = time.Time{}
	return out
}

