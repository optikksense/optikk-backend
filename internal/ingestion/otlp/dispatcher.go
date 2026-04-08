package otlp

import (
	"log/slog"
	"sync/atomic"
)

// TelemetryBatch wraps a set of typed telemetry rows with metadata.
type TelemetryBatch[T any] struct {
	TeamID int64
	Rows   []T
}

// Dispatcher fans out incoming OTLP batches to both persistence (ClickHouse)
// and real-time streaming (Live Tail) channels.
type Dispatcher[T any] struct {
	PersistenceChan chan TelemetryBatch[T]
	StreamingChan   chan TelemetryBatch[T]

	droppedCount int64
}

func NewDispatcher[T any](bufferSize int) *Dispatcher[T] {
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	return &Dispatcher[T]{
		PersistenceChan: make(chan TelemetryBatch[T], bufferSize),
		StreamingChan:   make(chan TelemetryBatch[T], bufferSize),
	}
}

func (d *Dispatcher[T]) Dispatch(batch TelemetryBatch[T]) {
	if len(batch.Rows) == 0 {
		return
	}

	// 1. Send to persistence (non-blocking)
	select {
	case d.PersistenceChan <- batch:
	default:
		// Drop if buffer full
		dropCount := atomic.AddInt64(&d.droppedCount, 1)
		if dropCount%100 == 1 { // Log every 100 drops
			slog.Error("ingest: Dispatcher persistence channel full, dropping batch",
				slog.Int64("dropped_batches_total", dropCount))
		}
	}

	// 2. Send to streaming (non-blocking)
	select {
	case d.StreamingChan <- batch:
	default:
		// Drop if buffer full
	}
}

func (d *Dispatcher[T]) Close() {
	close(d.PersistenceChan)
	close(d.StreamingChan)
}
