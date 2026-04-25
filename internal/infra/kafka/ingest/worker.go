package ingest

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Item pairs one signal payload with the *kgo.Record it was decoded from so
// the worker can commit offsets after the CH write succeeds. Raw may be nil
// during tests.
type Item[T any] struct {
	Payload T
	Raw     *kgo.Record
}

// Committer is the narrow interface the worker needs to commit offsets after a
// successful CH write. Provided by the dispatcher (wraps kgo.Client).
type Committer interface {
	CommitRecords(ctx context.Context, records ...*kgo.Record) error
}

// Worker owns one partition. Add() is called by the dispatcher to hand over
// records; tick + flush run inside Run().
type Worker[T any] struct {
	name      string
	partition string
	in        chan Item[T]
	writer    *Writer[T]
	acc       *Accumulator[Item[T]]
	committer Committer
}

// NewWorker builds a worker. queueSize caps the in-flight records awaiting
// flush; dispatcher uses this to decide when to pause the partition.
func NewWorker[T any](
	name string,
	queueSize int,
	writer *Writer[T],
	cfg AccumulatorConfig,
	sizeFn func(Item[T]) int,
	committer Committer,
) *Worker[T] {
	if queueSize <= 0 {
		queueSize = 4096
	}
	return &Worker[T]{
		name:      name,
		in:        make(chan Item[T], queueSize),
		writer:    writer,
		acc:       NewAccumulator[Item[T]](cfg, sizeFn),
		committer: committer,
	}
}

// SetPartition attaches the partition id used as the per-partition metric
// label. Called by the dispatcher once the (topic, partition) pair is known;
// kept off the constructor to avoid exposing partition routing to callers.
func (w *Worker[T]) SetPartition(p int32) {
	w.partition = strconv.FormatInt(int64(p), 10)
}

// Inbox exposes the channel so the dispatcher can enqueue items.
func (w *Worker[T]) Inbox() chan<- Item[T] { return w.in }

// Depth returns the approximate number of pending items awaiting flush. Used
// by the dispatcher as the backpressure signal for Pause/Resume.
func (w *Worker[T]) Depth() int { return len(w.in) }

// Run blocks until ctx is canceled. One goroutine per worker per partition.
// Ticker is intentionally a 100ms cadence — fine-grained enough for the 250ms
// MaxAge default, infrequent enough that idle partitions don't churn Prom
// series. The queue-depth gauge is sampled on every tick.
func (w *Worker[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		// Priority check: if ctx is already canceled don't let the ticker or
		// inbox case win the next select. Without this, the Go runtime can
		// non-deterministically pick the ticker case when both ctx.Done() and
		// ticker.C are ready, causing flush() to be called with a canceled ctx.
		// That fails immediately, the batch is dropped from the accumulator, and
		// a misleading "offsets NOT committed" warning is logged — all before
		// flushAll() on the shutdown path ever runs.
		select {
		case <-ctx.Done():
			w.drainOnShutdown()
			return
		default:
		}

		select {
		case <-ctx.Done():
			w.drainOnShutdown()
			return
		case it := <-w.in:
			if batch, reason, ok := w.acc.Add(it); ok {
				w.flush(ctx, batch, reason)
			}
		case now := <-ticker.C:
			w.observeDepth()
			if batch, reason, ok := w.acc.Due(now); ok {
				w.flush(ctx, batch, reason)
			}
		}
	}
}

func (w *Worker[T]) drainOnShutdown() {
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.flushAll(flushCtx)
}

func (w *Worker[T]) observeDepth() {
	if w.partition == "" {
		return
	}
	WorkerQueueDepth.WithLabelValues(w.name, w.partition).Set(float64(len(w.in)))
}

func (w *Worker[T]) flush(ctx context.Context, batch []Item[T], reason FlushReason) {
	if len(batch) == 0 {
		return
	}
	FlushRows.WithLabelValues(w.name, reason.String()).Observe(float64(len(batch)))
	BatchBytes.WithLabelValues(w.name).Observe(float64(w.acc.BytesAtFlush()))

	payloads := make([]T, len(batch))
	for i, it := range batch {
		payloads[i] = it.Payload
	}
	start := time.Now()
	err := w.writer.Write(ctx, payloads)
	elapsed := time.Since(start).Seconds()
	if err != nil {
		FlushDuration.WithLabelValues(w.name, "err").Observe(elapsed)
		slog.WarnContext(ctx, "ingest worker: write error; offsets NOT committed",
			slog.String("signal", w.name), slog.Any("error", err))
		return
	}
	FlushDuration.WithLabelValues(w.name, "ok").Observe(elapsed)
	w.commit(ctx, batch)
}

func (w *Worker[T]) commit(ctx context.Context, batch []Item[T]) {
	if w.committer == nil {
		return
	}
	raws := make([]*kgo.Record, 0, len(batch))
	for _, it := range batch {
		if it.Raw != nil {
			raws = append(raws, it.Raw)
		}
	}
	if len(raws) == 0 {
		return
	}
	if err := w.committer.CommitRecords(ctx, raws...); err != nil {
		slog.WarnContext(ctx, "ingest worker: commit failed",
			slog.String("signal", w.name), slog.Any("error", err))
	}
}

func (w *Worker[T]) flushAll(ctx context.Context) {
	if batch := w.acc.DrainAll(); len(batch) > 0 {
		w.flush(ctx, batch, FlushStop)
	}
}
