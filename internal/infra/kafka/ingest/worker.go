package ingest

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Item pairs one signal payload with the *kgo.Record it was decoded from so
// the worker can commit offsets after the CH write succeeds. Raw may be nil
// during tests.
type Item[T any] struct {
	Payload	T
	Raw	*kgo.Record
}

// Committer is the narrow interface the worker needs to commit offsets after a
// successful CH write. Provided by the dispatcher (wraps kgo.Client).
type Committer interface {
	CommitRecords(ctx context.Context, records ...*kgo.Record) error
}

// Worker owns one partition. Add() is called by the dispatcher to hand over
// records; tick + flush run inside Run().
type Worker[T any] struct {
	name		string
	in		chan Item[T]
	writer		*Writer[T]
	acc		*Accumulator[Item[T]]
	committer	Committer
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
		queueSize = 1024
	}
	return &Worker[T]{
		name:		name,
		in:		make(chan Item[T], queueSize),
		writer:		writer,
		acc:		NewAccumulator[Item[T]](cfg, sizeFn),
		committer:	committer,
	}
}

// Inbox exposes the channel so the dispatcher can enqueue items.
func (w *Worker[T]) Inbox() chan<- Item[T]	{ return w.in }

// Depth returns the approximate number of pending items awaiting flush. Used
// by the dispatcher as the backpressure signal for Pause/Resume.
func (w *Worker[T]) Depth() int	{ return len(w.in) }

// Run blocks until ctx is canceled. One goroutine per worker per partition.
func (w *Worker[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			w.flushAll(flushCtx)
			return
		case it := <-w.in:
			if batch, _, ok := w.acc.Add(it); ok {
				w.flush(ctx, batch)
			}
		case now := <-ticker.C:
			if batch, ok := w.acc.Due(now); ok {
				w.flush(ctx, batch)
			}
		}
	}
}

func (w *Worker[T]) flush(ctx context.Context, batch []Item[T]) {
	if len(batch) == 0 {
		return
	}
	payloads := make([]T, len(batch))
	for i, it := range batch {
		payloads[i] = it.Payload
	}
	if err := w.writer.Write(ctx, payloads); err != nil {
		slog.WarnContext(ctx, "ingest worker: write error; offsets NOT committed",
			slog.String("signal", w.name), slog.Any("error", err))
		return
	}
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
		w.flush(ctx, batch)
	}
}
