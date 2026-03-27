package ingest

import (
	"errors"
	"sync/atomic"
	"time"
)

const (
	DefaultBatchSize = 1000
	DefaultFlushMs   = 500
	DefaultCapacity  = 1 << 17
)

var ErrBackpressure = errors.New("ingest: backpressure — queue full")

type Row struct{ Values []any }

type FlushFunc func(batch []Row)

type Option func(*Queue)

func WithBatchSize(n int) Option      { return func(q *Queue) { q.batchSize = n } }
func WithFlushInterval(ms int) Option { return func(q *Queue) { q.flushMs = ms } }
func WithCapacity(n int) Option       { return func(q *Queue) { q.capacity = n } }

type Queue struct {
	ch        chan Row
	flushFn   FlushFunc
	batchSize int
	flushMs   int
	capacity  int
	closed    atomic.Bool
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func NewQueue(flush FlushFunc, opts ...Option) *Queue {
	q := &Queue{
		flushFn:   flush,
		batchSize: DefaultBatchSize,
		flushMs:   DefaultFlushMs,
		capacity:  DefaultCapacity,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	for _, o := range opts {
		o(q)
	}
	q.ch = make(chan Row, q.capacity)
	go q.worker()
	return q
}

func (q *Queue) Enqueue(rows []Row) error {
	if q.closed.Load() {
		return errors.New("ingest: queue closed")
	}
	for _, row := range rows {
		select {
		case q.ch <- row:
		default:
			return ErrBackpressure
		}
	}
	return nil
}

func (q *Queue) QueueLen() int { return len(q.ch) }

func (q *Queue) Close() error {
	q.closed.Store(true)
	close(q.stopCh)
	<-q.doneCh
	if batch := q.drain(); len(batch) > 0 {
		q.flushFn(batch)
	}
	return nil
}

func (q *Queue) worker() {
	defer close(q.doneCh)
	ticker := time.NewTicker(time.Duration(q.flushMs) * time.Millisecond)
	defer ticker.Stop()

	buf := make([]Row, 0, q.batchSize)
	for {
		select {
		case <-q.stopCh:
			return
		case row := <-q.ch:
			buf = append(buf, row)
			buf = q.drainInto(buf)
			if len(buf) >= q.batchSize {
				q.flushFn(buf)
				buf = buf[:0]
			}
		case <-ticker.C:
			if len(buf) > 0 {
				q.flushFn(buf)
				buf = buf[:0]
			}
		}
	}
}

func (q *Queue) drainInto(buf []Row) []Row {
	for len(buf) < q.batchSize {
		select {
		case r := <-q.ch:
			buf = append(buf, r)
		default:
			return buf
		}
	}
	return buf
}

func (q *Queue) drain() []Row {
	var rows []Row
	for {
		select {
		case r := <-q.ch:
			rows = append(rows, r)
		default:
			return rows
		}
	}
}
