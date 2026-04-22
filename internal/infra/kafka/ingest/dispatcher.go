package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Decoder converts a raw Kafka record to a signal-specific payload. Returning
// err drops the record with a log line (malformed protobuf is not retriable).
type Decoder[T any] func(r *kgo.Record) (T, error)

// WorkerFactory produces a fresh worker the first time the dispatcher sees a
// (topic, partition) pair. Same partition reuses its existing worker across
// cooperative rebalances.
type WorkerFactory[T any] func() *Worker[T]

// Dispatcher drives one PollFetches loop per signal and fans records out to
// per-partition workers. The hot path is intentionally simple — all retry +
// batching + commit logic lives in Worker and Writer.
type Dispatcher[T any] struct {
	name    string
	client  *kgo.Client
	decode  Decoder[T]
	factory WorkerFactory[T]

	// pauseDepth triggers Pause once a worker's inbox goes above this. Set via
	// DefaultDispatcherOptions. Pause/Resume are keyed by (topic, partition).
	pauseDepth  int
	resumeDepth int

	mu      sync.Mutex
	workers map[partKey]*Worker[T]
	paused  map[partKey]struct{}
	cancels map[partKey]context.CancelFunc
	wg      sync.WaitGroup
}

type partKey struct {
	topic     string
	partition int32
}

// DispatcherOptions tunes backpressure thresholds.
type DispatcherOptions struct {
	PauseDepth  int
	ResumeDepth int
}

// DefaultDispatcherOptions pauses at 80% and resumes at 40% of the default
// worker queue size (1024). Those percentages keep Pause/Resume from
// flapping under steady load.
func DefaultDispatcherOptions() DispatcherOptions {
	return DispatcherOptions{PauseDepth: 819, ResumeDepth: 410}
}

// NewDispatcher constructs a dispatcher around an existing kgo.Client.
func NewDispatcher[T any](name string, client *kgo.Client, decode Decoder[T], factory WorkerFactory[T], opts DispatcherOptions) *Dispatcher[T] {
	if opts.PauseDepth <= 0 {
		opts = DefaultDispatcherOptions()
	}
	return &Dispatcher[T]{
		name: name, client: client, decode: decode, factory: factory,
		pauseDepth: opts.PauseDepth, resumeDepth: opts.ResumeDepth,
		workers: map[partKey]*Worker[T]{},
		paused:  map[partKey]struct{}{},
		cancels: map[partKey]context.CancelFunc{},
	}
}

// Run blocks until ctx is canceled. Spawns worker goroutines on first sight
// of a partition; each worker drains until its context (scoped to Run) ends.
func (d *Dispatcher[T]) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			d.shutdown()
			return
		}
		fetches := d.client.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, kgo.ErrClientClosed) {
				d.shutdown()
				return
			}
		}
		fetches.EachError(func(topic string, p int32, err error) {
			slog.Warn("ingest dispatcher: partition fetch error",
				slog.String("signal", d.name),
				slog.String("topic", topic),
				slog.Int("partition", int(p)),
				slog.Any("error", err))
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			d.route(ctx, p)
		})
		d.evaluatePauseResume()
	}
}

func (d *Dispatcher[T]) route(ctx context.Context, p kgo.FetchTopicPartition) {
	if len(p.Records) == 0 {
		return
	}
	w := d.workerFor(ctx, partKey{p.Topic, p.Partition})
	for _, r := range p.Records {
		payload, err := d.decode(r)
		if err != nil {
			slog.Warn("ingest dispatcher: decode dropped one record",
				slog.String("signal", d.name), slog.Any("error", err))
			continue
		}
		w.Inbox() <- Item[T]{Payload: payload, Raw: r}
	}
}

func (d *Dispatcher[T]) workerFor(parent context.Context, k partKey) *Worker[T] {
	d.mu.Lock()
	defer d.mu.Unlock()
	if w, ok := d.workers[k]; ok {
		return w
	}
	w := d.factory()
	d.workers[k] = w
	wctx, cancel := context.WithCancel(parent)
	d.cancels[k] = cancel
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		w.Run(wctx)
	}()
	return w
}

func (d *Dispatcher[T]) evaluatePauseResume() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, w := range d.workers {
		if _, isPaused := d.paused[k]; isPaused {
			if w.Depth() <= d.resumeDepth {
				d.client.ResumeFetchPartitions(map[string][]int32{k.topic: {k.partition}})
				delete(d.paused, k)
			}
			continue
		}
		if w.Depth() >= d.pauseDepth {
			d.client.PauseFetchPartitions(map[string][]int32{k.topic: {k.partition}})
			d.paused[k] = struct{}{}
		}
	}
}

func (d *Dispatcher[T]) shutdown() {
	d.mu.Lock()
	for _, c := range d.cancels {
		c()
	}
	d.mu.Unlock()
	d.wg.Wait()
}
