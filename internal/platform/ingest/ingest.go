// Package ingest provides a batched, backpressure-aware insert pipeline for
// ClickHouse. Rows are enqueued via Enqueue and flushed by a background worker
// when either the batch size (1000 rows) or flush interval (500ms) is reached.
//
// Backpressure: when the queue exceeds MaxQueueSize (10,000 rows), Enqueue
// returns ErrBackpressure. HTTP handlers should map this to 429 + Retry-After.
//
// All inserts use async_insert=1 on the ClickHouse connection. Never insert
// from the HTTP hot path — always queue → worker → ClickHouse.
package ingest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func init() {
	gob.Register([]any{})
	gob.Register(map[string]string{})
	gob.Register(map[string]any{})
	gob.Register(time.Time{})
}

// gobBufPool reuses bytes.Buffer instances for gob encoding in the Kafka path.
var gobBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// Defaults — all tuneable via Option functions.
const (
	DefaultBatchSize    = 1000
	DefaultFlushMs      = 500
	DefaultMaxQueueSize = 10_000
)

// ErrBackpressure is returned by Enqueue when the internal queue exceeds
// MaxQueueSize. Callers should respond with HTTP 429 and a Retry-After header.
var ErrBackpressure = errors.New("ingest: backpressure — queue full")

// Row is a generic row to be inserted. The Values slice must match the column
// order of the target table.
type Row struct {
	Values []any
}

// Queue is a batched insert queue with backpressure support.
type Queue struct {
	db           *sql.DB
	table        string
	columns      []string
	batchSize    int
	flushMs      int
	maxQueueSize int

	mu     sync.Mutex
	buf    []Row
	closed bool
	stopCh chan struct{}
	doneCh chan struct{}

	useKafka    bool
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader
}

// Option configures a Queue.
type Option func(*Queue)

// WithBatchSize overrides the default batch size (1000).
func WithBatchSize(n int) Option {
	return func(q *Queue) { q.batchSize = n }
}

// WithFlushInterval overrides the default flush interval (500ms).
func WithFlushInterval(ms int) Option {
	return func(q *Queue) { q.flushMs = ms }
}

// WithMaxQueueSize overrides the default max queue size (10,000).
func WithMaxQueueSize(n int) Option {
	return func(q *Queue) { q.maxQueueSize = n }
}

// NewQueue creates a new batched insert queue and starts the background worker.
// The table must be fully qualified (e.g. "observability.spans").
// Columns must match the column order of the VALUES clause.
func NewQueue(db *sql.DB, useKafka bool, brokers []string, table string, columns []string, opts ...Option) *Queue {
	q := &Queue{
		db:           db,
		table:        table,
		columns:      columns,
		batchSize:    DefaultBatchSize,
		flushMs:      DefaultFlushMs,
		maxQueueSize: DefaultMaxQueueSize,
		buf:          make([]Row, 0, DefaultBatchSize),
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		useKafka:     useKafka && len(brokers) > 0,
	}
	for _, o := range opts {
		o(q)
	}

	if q.useKafka {
		q.kafkaWriter = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    table,
			Balancer: &kafka.LeastBytes{},
		}
		q.kafkaReader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  "ingest-worker-group",
			Topic:    table,
			MinBytes: 10e3,
			MaxBytes: 10e6,
		})
	}

	go q.worker()
	return q
}

// Enqueue adds rows to the internal buffer. Returns ErrBackpressure when the
// queue exceeds MaxQueueSize — the caller should respond with HTTP 429 and
// include a Retry-After header.
func (q *Queue) Enqueue(rows []Row) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errors.New("ingest: queue closed")
	}
	q.mu.Unlock()

	if q.useKafka {
		msgs := make([]kafka.Message, len(rows))
		for i, r := range rows {
			buf := gobBufPool.Get().(*bytes.Buffer)
			buf.Reset()
			enc := gob.NewEncoder(buf)
			if err := enc.Encode(r.Values); err != nil {
				gobBufPool.Put(buf)
				return err
			}
			// Copy bytes before returning buffer to pool.
			val := make([]byte, buf.Len())
			copy(val, buf.Bytes())
			gobBufPool.Put(buf)
			msgs[i] = kafka.Message{Value: val}
		}
		if err := q.kafkaWriter.WriteMessages(context.Background(), msgs...); err != nil {
			return fmt.Errorf("ingest: kafka write failed: %v", err)
		}
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.buf)+len(rows) > q.maxQueueSize {
		return ErrBackpressure
	}

	q.buf = append(q.buf, rows...)
	return nil
}

// QueueLen returns the current number of buffered rows.
func (q *Queue) QueueLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.buf)
}

// Close stops the background worker and flushes any remaining rows.
func (q *Queue) Close() error {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()

	close(q.stopCh)
	<-q.doneCh

	// Final flush.
	q.mu.Lock()
	batch := q.drainLocked()
	q.mu.Unlock()

	if len(batch) > 0 {
		return q.flush(batch)
	}

	if q.useKafka {
		q.kafkaWriter.Close()
		q.kafkaReader.Close()
	}

	return nil
}

// worker runs in a goroutine. It flushes when the batch is full or the timer
// fires (500ms), whichever comes first.
func (q *Queue) worker() {
	defer close(q.doneCh)

	if q.useKafka {
		q.kafkaWorker()
		return
	}

	ticker := time.NewTicker(time.Duration(q.flushMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-q.stopCh:
			return
		case <-ticker.C:
			q.mu.Lock()
			batch := q.drainLocked()
			q.mu.Unlock()

			if len(batch) > 0 {
				if err := q.flush(batch); err != nil {
					log.Printf("ingest: worker flush error (%s): %v", q.table, err)
				}
			}
		default:
			q.mu.Lock()
			ready := len(q.buf) >= q.batchSize
			var batch []Row
			if ready {
				batch = q.drainBatchLocked()
			}
			q.mu.Unlock()

			if ready {
				if err := q.flush(batch); err != nil {
					log.Printf("ingest: worker batch flush error (%s): %v", q.table, err)
				}
			} else {
				// Avoid busy-spin: sleep a short interval before re-checking.
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (q *Queue) kafkaWorker() {
	ticker := time.NewTicker(time.Duration(q.flushMs) * time.Millisecond)
	defer ticker.Stop()

	batch := make([]Row, 0, q.batchSize)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-q.stopCh
		cancel()
	}()

	for {
		m, err := q.kafkaReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Printf("ingest: kafka read error (%s): %v", q.table, err)
			time.Sleep(1 * time.Second)
			continue
		}

		var values []any
		dec := gob.NewDecoder(bytes.NewReader(m.Value))
		if err := dec.Decode(&values); err == nil {
			batch = append(batch, Row{Values: values})
		} else {
			log.Printf("ingest: kafka gob decode error: %v", err)
		}

		if len(batch) >= q.batchSize {
			if err := q.flush(batch); err != nil {
				log.Printf("ingest: worker batch flush error (%s): %v", q.table, err)
			}
			batch = make([]Row, 0, q.batchSize)
		}

		select {
		case <-ticker.C:
			if len(batch) > 0 {
				if err := q.flush(batch); err != nil {
					log.Printf("ingest: worker timer flush error (%s): %v", q.table, err)
				}
				batch = make([]Row, 0, q.batchSize)
			}
		default:
		}
	}

	if len(batch) > 0 {
		if err := q.flush(batch); err != nil {
			log.Printf("ingest: worker shutdown flush error (%s): %v", q.table, err)
		}
	}
}

// drainLocked moves all buffered rows out. Caller must hold mu.
func (q *Queue) drainLocked() []Row {
	if len(q.buf) == 0 {
		return nil
	}
	batch := q.buf
	q.buf = make([]Row, 0, q.batchSize)
	return batch
}

// drainBatchLocked moves at most batchSize rows out. Caller must hold mu.
// Uses sub-slicing instead of copying to avoid allocation.
func (q *Queue) drainBatchLocked() []Row {
	n := q.batchSize
	if n > len(q.buf) {
		n = len(q.buf)
	}
	batch := q.buf[:n:n]
	q.buf = q.buf[n:]
	return batch
}

// flush writes a batch to ClickHouse in a single INSERT with async_insert=1.
// Batch inserts only: 1000 rows or 500ms minimum.
func (q *Queue) flush(batch []Row) error {
	if len(batch) == 0 {
		return nil
	}

	colList := strings.Join(q.columns, ", ")
	placeholders := "(" + strings.Repeat("?, ", len(q.columns)-1) + "?)"
	rowPlaceholders := make([]string, len(batch))
	args := make([]any, 0, len(batch)*len(q.columns))

	for i, row := range batch {
		rowPlaceholders[i] = placeholders
		args = append(args, row.Values...)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) SETTINGS async_insert=1, wait_for_async_insert=1 VALUES %s",
		q.table, colList, strings.Join(rowPlaceholders, ", "),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := q.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Printf("ERROR: ClickHouse insert failed for table %s: %v", q.table, err)
	} else {
		log.Printf("ingest: successfully flushed %d rows to %s", len(batch), q.table)
	}
	return err
}
