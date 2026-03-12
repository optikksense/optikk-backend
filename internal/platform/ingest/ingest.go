// Package ingest is a batched, backpressure-aware ClickHouse insert pipeline.
// Rows are queued via Enqueue and flushed by a background worker.
// Return ErrBackpressure → HTTP 429 + Retry-After when the ring is full.
package ingest

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// RingCapacity is the fixed ring buffer size — must be a power of 2 so index
// wrapping uses idx & (RingCapacity-1) instead of the slower % operator.
const RingCapacity = 1 << 17 // 131 072 slots

const (
	DefaultBatchSize    = 1000
	DefaultFlushMs      = 500
	DefaultMaxQueueSize = 10_000
)

var ErrBackpressure = errors.New("ingest: backpressure — queue full")

// Row is a single row to insert. Values must match the target table's column order.
type Row struct {
	Values []any
}

type Queue struct {
	slots [RingCapacity]atomic.Value // pre-allocated slots; each holds a *Row
	head  atomic.Uint64              // claimed by producers via CAS
	tail  atomic.Uint64              // advanced by the single consumer

	conn        clickhouse.Conn // native driver connection
	queryPrefix string          // "INSERT INTO table (col, ...)" — built once
	flushSem    chan struct{}    // limits concurrent flushes to cap(flushSem)

	table     string
	columns   []string
	batchSize int
	flushMs   int

	closed atomic.Bool // set to true in Close(); no mutex needed

	stopCh chan struct{}
	doneCh chan struct{}
}

// Option configures a Queue.
type Option func(*Queue)

// WithBatchSize overrides the default batch size (1000).
func WithBatchSize(n int) Option { return func(q *Queue) { q.batchSize = n } }

// WithFlushInterval overrides the default flush interval (500ms).
func WithFlushInterval(ms int) Option { return func(q *Queue) { q.flushMs = ms } }

func WithMaxQueueSize(_ int) Option { return func(_ *Queue) {} }

// NewQueue creates the queue and starts the background worker.
// table must be fully qualified (e.g. "observability.spans").
func NewQueue(conn clickhouse.Conn, table string, columns []string, opts ...Option) *Queue {
	q := &Queue{
		conn:      conn,
		table:     table,
		columns:   columns,
		batchSize: DefaultBatchSize,
		flushMs:   DefaultFlushMs,
		flushSem:  make(chan struct{}, 4), // at most 4 concurrent flushes
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	for _, o := range opts {
		o(q)
	}
	// Build the INSERT prefix once; flush never does string work.
	q.queryPrefix = "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ")"
	go q.worker()
	return q
}

// publish claims a ring slot via a CAS loop and stores the row.
// Many goroutines compete; exactly one wins each slot per iteration.
func (q *Queue) publish(row Row) error {
	for {
		h := q.head.Load()
		t := q.tail.Load()

		if h-t >= RingCapacity { // every slot occupied → backpressure
			log.Printf("ingest: BACKPRESSURE table=%s depth=%d/%d", q.table, h-t, RingCapacity)
			return ErrBackpressure
		}

		// Race to claim slot h; loop if another producer got there first.
		if !q.head.CompareAndSwap(h, h+1) {
			continue
		}

		// Bitmask wrapping — safe because RingCapacity is a power of 2.
		idx := h & (RingCapacity - 1)
		q.slots[idx].Store(&row)
		return nil
	}
}

// consume drains all ready slots from tail to head. Single-reader only.
func (q *Queue) consume() []Row {
	t := q.tail.Load()
	h := q.head.Load()
	if t == h {
		return nil
	}

	batch := make([]Row, 0, h-t)
	for t < h {
		idx := t & (RingCapacity - 1)
		v := q.slots[idx].Load()

		// Sequence barrier: producer claimed this slot but hasn't stored yet — stop.
		if v == nil {
			break
		}

		batch = append(batch, *v.(*Row))
		q.slots[idx].Store((*Row)(nil)) // zero slot for future use
		t++
	}

	q.tail.Store(t)
	return batch
}

// Enqueue adds rows to the ring. Returns ErrBackpressure when the ring is full.
func (q *Queue) Enqueue(rows []Row) error {
	if q.closed.Load() {
		return errors.New("ingest: queue closed")
	}
	for _, row := range rows {
		if err := q.publish(row); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) QueueLen() int {
	h := q.head.Load()
	t := q.tail.Load()
	if h <= t {
		return 0
	}
	return int(h - t)
}

// Close stops the worker, waits for all in-flight flushes, then returns.
func (q *Queue) Close() error {
	q.closed.Store(true)
	close(q.stopCh)
	<-q.doneCh // wait for worker to exit

	// Final drain — no goroutines racing at this point.
	if batch := q.consume(); len(batch) > 0 {
		if err := q.flush(batch); err != nil {
			log.Printf("ingest: Close flush error (%s): %v", q.table, err)
		}
	}

	// Acquire all semaphore slots — blocks until every in-flight flush finishes.
	for i := 0; i < cap(q.flushSem); i++ {
		q.flushSem <- struct{}{}
	}
	return nil
}

func (q *Queue) worker() {
	defer close(q.doneCh)

	ticker := time.NewTicker(time.Duration(q.flushMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-q.stopCh:
			return

		case <-ticker.C:
			if batch := q.consume(); len(batch) > 0 {
				q.flushAsync(batch)
			}

		default:
			if q.QueueLen() >= q.batchSize {
				if batch := q.consume(); len(batch) > 0 {
					q.flushAsync(batch)
				}
			} else {
				// Yield 1ms — keeps CPU low; the 500ms ticker is the primary flush trigger.
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

// flushAsync acquires a semaphore slot, then flushes in a goroutine.
func (q *Queue) flushAsync(batch []Row) {
	q.flushSem <- struct{}{} // acquire — blocks if 4 flushes already running
	go func() {
		defer func() { <-q.flushSem }() // release when done
		if err := q.flush(batch); err != nil {
			log.Printf("ingest: flush error (%s): %v", q.table, err)
		}
	}()
}

// flush sends a batch to ClickHouse via the native PrepareBatch API.
func (q *Queue) flush(batch []Row) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := q.conn.PrepareBatch(ctx, q.queryPrefix)
	if err != nil {
		log.Printf("ingest: PrepareBatch error (%s): %v", q.table, err)
		return err
	}

	for _, row := range batch {
		if err := b.Append(row.Values...); err != nil {
			log.Printf("ingest: Append error (%s): %v", q.table, err)
			return err
		}
	}

	if err := b.Send(); err != nil {
		log.Printf("ERROR: ClickHouse batch send failed for table %s: %v", q.table, err)
		return err
	}

	log.Printf("ingest: flushed %d rows to %s (took %v, queue_depth=%d)", len(batch), q.table, time.Since(start), q.QueueLen())
	return nil
}
