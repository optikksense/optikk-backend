package ingestion

import (
	"log/slog"
	"sync/atomic"

	platformingestion "github.com/Optikk-Org/optikk-backend/internal/platform/ingestion"
)

type LocalDispatcher[T any] struct {
	persistenceChan chan platformingestion.TelemetryBatch[T]
	streamingChan   chan platformingestion.TelemetryBatch[T]
	droppedCount    int64
}

func NewLocalDispatcher[T any](bufferSize int) platformingestion.Dispatcher[T] {
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	return &LocalDispatcher[T]{
		persistenceChan: make(chan platformingestion.TelemetryBatch[T], bufferSize),
		streamingChan:   make(chan platformingestion.TelemetryBatch[T], bufferSize),
	}
}

func (d *LocalDispatcher[T]) Dispatch(batch platformingestion.TelemetryBatch[T]) {
	if len(batch.Rows) == 0 {
		return
	}

	select {
	case d.persistenceChan <- batch:
	default:
		dropCount := atomic.AddInt64(&d.droppedCount, 1)
		if dropCount%100 == 1 {
			slog.Warn("ingest: dispatcher persistence channel full, dropping batch",
				slog.Int64("dropped_batches_total", dropCount))
		}
	}

	select {
	case d.streamingChan <- batch:
	default:
	}
}

func (d *LocalDispatcher[T]) Persistence() <-chan platformingestion.TelemetryBatch[T] {
	return d.persistenceChan
}

func (d *LocalDispatcher[T]) Streaming() <-chan platformingestion.TelemetryBatch[T] {
	return d.streamingChan
}

func (d *LocalDispatcher[T]) Close() {
	close(d.persistenceChan)
	close(d.streamingChan)
}
