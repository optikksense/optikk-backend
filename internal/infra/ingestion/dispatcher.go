package ingestion

import (
	"log/slog"
	"sync/atomic"
)

type LocalDispatcher[T any] struct {
	persistenceChan chan TelemetryBatch[T]
	streamingChan   chan TelemetryBatch[T]
	droppedCount    int64
}

func NewLocalDispatcher[T any](bufferSize int) Dispatcher[T] {
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	return &LocalDispatcher[T]{
		persistenceChan: make(chan TelemetryBatch[T], bufferSize),
		streamingChan:   make(chan TelemetryBatch[T], bufferSize),
	}
}

func (d *LocalDispatcher[T]) Dispatch(batch TelemetryBatch[T]) {
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

func (d *LocalDispatcher[T]) Persistence() <-chan TelemetryBatch[T] {
	return d.persistenceChan
}

func (d *LocalDispatcher[T]) Streaming() <-chan TelemetryBatch[T] {
	return d.streamingChan
}

func (d *LocalDispatcher[T]) Close() {
	close(d.persistenceChan)
	close(d.streamingChan)
}
