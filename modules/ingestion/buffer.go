package telemetry

import (
	"context"
	"log"
	"sync"
	"time"
)

// AsyncBuffer accumulates items in memory and flushes them to ClickHouse in the
// background. HTTP handlers call Append() and return immediately; the buffer
// handles durability asynchronously. Flushes are triggered by size threshold
// or wall-clock interval, whichever comes first.
type AsyncBuffer[T any] struct {
	mu      sync.Mutex
	buf     []T
	max     int
	flushFn func(context.Context, []T) error
	done    chan struct{}
	wg      sync.WaitGroup
}

func newAsyncBuffer[T any](max int, interval time.Duration, flushFn func(context.Context, []T) error) *AsyncBuffer[T] {
	b := &AsyncBuffer[T]{
		max:     max,
		flushFn: flushFn,
		done:    make(chan struct{}),
	}
	b.wg.Add(1)
	go b.loop(interval)
	return b
}

// Append adds items to the buffer. If the buffer is full it triggers an immediate flush.
func (b *AsyncBuffer[T]) Append(items []T) {
	b.mu.Lock()
	b.buf = append(b.buf, items...)
	needsFlush := len(b.buf) >= b.max
	b.mu.Unlock()
	if needsFlush {
		b.triggerFlush()
	}
}

// Close flushes remaining items and stops the background goroutine.
func (b *AsyncBuffer[T]) Close() {
	close(b.done)
	b.wg.Wait()
}

func (b *AsyncBuffer[T]) loop(interval time.Duration) {
	defer b.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.triggerFlush()
		case <-b.done:
			b.triggerFlush()
			return
		}
	}
}

func (b *AsyncBuffer[T]) triggerFlush() {
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return
	}
	items := b.buf
	b.buf = make([]T, 0, b.max)
	b.mu.Unlock()

	if err := b.flushFn(context.Background(), items); err != nil {
		log.Printf("buffer: flush error (%d items): %v", len(items), err)
	}
}
