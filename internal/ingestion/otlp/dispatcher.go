package otlp

import (
	"log/slog"
	"sync/atomic"

	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/internal/ingest"
)

type SignalType int

const (
	SignalLog SignalType = iota
	SignalSpan
	SignalMetric
)

// TelemetryBatch wraps a set of rows with metadata for the dispatcher.
type TelemetryBatch struct {
	Signal SignalType
	TeamID int64
	Rows   []ingest.Row
}

// Dispatcher fans out incoming OTLP batches to both persistence (ClickHouse)
// and real-time streaming (Live Tail) channels.
type Dispatcher struct {
	PersistenceChan chan TelemetryBatch
	StreamingChan   chan TelemetryBatch

	droppedCount int64
}

func NewDispatcher(bufferSize int) *Dispatcher {
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	return &Dispatcher{
		PersistenceChan: make(chan TelemetryBatch, bufferSize),
		StreamingChan:   make(chan TelemetryBatch, bufferSize),
	}
}

func (d *Dispatcher) Dispatch(batch TelemetryBatch) {
	if len(batch.Rows) == 0 {
		return
	}

	// 1. Send to persistence (non-blocking)
	select {
	case d.PersistenceChan <- batch:
	default:
		// Drop if buffer full
		dropCount := atomic.AddInt64(&d.droppedCount, 1)
		if dropCount%100 == 1 { // Log every 100 drops to avoid log flooding
			slog.Warn("ingest: Dispatcher persistence channel full, dropping batch",
				slog.Int("signal", int(batch.Signal)),
				slog.Int64("dropped_batches_total", dropCount))
		}
	}

	// 2. Send to streaming (non-blocking)
	select {
	case d.StreamingChan <- batch:
	default:
		// Drop if buffer full (streaming usually high volume, we don't log separate counter for now)
	}
}

func (d *Dispatcher) Close() {
	close(d.PersistenceChan)
	close(d.StreamingChan)
}
