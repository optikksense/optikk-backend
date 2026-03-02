package telemetry

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// maxRetries is the number of flush attempts before records are dropped.
	maxRetries = 3
	// retryBaseDelay is the initial delay between retry attempts (doubles each retry).
	retryBaseDelay = 500 * time.Millisecond
	// defaultMaxBufferCapacity is the hard upper bound on buffered items.
	// If the buffer reaches this size, new Append calls drop the oldest items
	// to prevent unbounded memory growth when the sink is slow/unavailable.
	defaultMaxBufferCapacity = 100_000
)

// AsyncBuffer accumulates items in memory and flushes them to ClickHouse in the
// background. HTTP handlers call Append() and return immediately; the buffer
// handles durability asynchronously. Flushes are triggered by size threshold
// or wall-clock interval, whichever comes first.
//
// Resilience features:
//   - Retry with exponential backoff on flush failure (up to maxRetries).
//   - Failed records are re-enqueued for the next flush cycle.
//   - Bounded capacity prevents OOM when the sink is unavailable.
//   - Dropped item counter exposed for monitoring.
type AsyncBuffer[T any] struct {
	mu      sync.Mutex
	buf     []T
	max     int // flush threshold (batch size)
	maxCap  int // hard upper bound on buffer length
	flushFn func(context.Context, []T) error
	done    chan struct{}
	wg      sync.WaitGroup

	// Metrics — read with atomic loads; no mutex needed.
	dropped  atomic.Int64
	flushed  atomic.Int64
	failures atomic.Int64
}

// BufferStats exposes buffer health metrics for monitoring.
type BufferStats struct {
	Pending  int   // items currently buffered
	Dropped  int64 // items dropped due to capacity overflow (cumulative)
	Flushed  int64 // items successfully flushed (cumulative)
	Failures int64 // flush attempts that failed (cumulative)
}

func newAsyncBuffer[T any](max int, interval time.Duration, flushFn func(context.Context, []T) error) *AsyncBuffer[T] {
	b := &AsyncBuffer[T]{
		max:     max,
		maxCap:  defaultMaxBufferCapacity,
		flushFn: flushFn,
		done:    make(chan struct{}),
	}
	b.wg.Add(1)
	go b.loop(interval)
	return b
}

// Append adds items to the buffer. If the buffer exceeds the hard capacity limit,
// the oldest items are dropped. If the flush threshold is reached, an immediate
// flush is triggered.
func (b *AsyncBuffer[T]) Append(items []T) {
	b.mu.Lock()
	b.buf = append(b.buf, items...)

	// Enforce hard capacity limit — drop oldest items.
	if len(b.buf) > b.maxCap {
		overflow := len(b.buf) - b.maxCap
		b.buf = b.buf[overflow:]
		b.dropped.Add(int64(overflow))
		log.Printf("buffer: dropped %d oldest items (capacity %d exceeded)", overflow, b.maxCap)
	}

	needsFlush := len(b.buf) >= b.max
	b.mu.Unlock()
	if needsFlush {
		b.triggerFlush()
	}
}

// Stats returns a snapshot of the buffer's health metrics.
func (b *AsyncBuffer[T]) Stats() BufferStats {
	b.mu.Lock()
	pending := len(b.buf)
	b.mu.Unlock()
	return BufferStats{
		Pending:  pending,
		Dropped:  b.dropped.Load(),
		Flushed:  b.flushed.Load(),
		Failures: b.failures.Load(),
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

	// Retry with exponential backoff.
	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := retryBaseDelay * time.Duration(1<<(attempt-1))
			time.Sleep(delay)
			log.Printf("buffer: retry %d/%d for %d items", attempt, maxRetries, len(items))
		}
		err = b.flushFn(context.Background(), items)
		if err == nil {
			b.flushed.Add(int64(len(items)))
			return
		}
		b.failures.Add(1)
	}

	// All retries exhausted — re-enqueue items for the next flush cycle.
	log.Printf("buffer: flush failed after %d retries (%d items re-enqueued): %v", maxRetries, len(items), err)
	b.mu.Lock()
	// Prepend failed items so they are flushed first next time.
	b.buf = append(items, b.buf...)

	// Enforce capacity limit on the combined buffer.
	if len(b.buf) > b.maxCap {
		overflow := len(b.buf) - b.maxCap
		b.buf = b.buf[overflow:]
		b.dropped.Add(int64(overflow))
		log.Printf("buffer: dropped %d oldest items after re-enqueue (capacity %d exceeded)", overflow, b.maxCap)
	}
	b.mu.Unlock()
}
