package logs

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Dispatcher is the logs-signal entrypoint for the generic ingest pipeline.
// It owns one PollFetches loop (via ingest.Dispatcher) and hands each
// partition off to a freshly-built Worker (via newLogsWorker) that composes
// the shared ingest.Worker/Writer generics around the CH batch insert plus
// DLQ sink defined in writer.go / dlq.go.
type Dispatcher struct {
	inner *ingest.Dispatcher[*Row]

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDispatcher wires the shared generic dispatcher around the logs-specific
// decoder, worker factory, CH batch insert, and DLQ sink.
func NewDispatcher(consumer *kafkainfra.Consumer, ch clickhouse.Conn, dlq *DLQProducer) *Dispatcher {
	writer := NewWriter(ch, dlq)
	factory := func() *ingest.Worker[*Row] {
		return newLogsWorker(writer, consumer)
	}
	d := ingest.NewDispatcher[*Row](
		"logs",
		consumer.Client(),
		decodeRecord,
		factory,
		ingest.DefaultDispatcherOptions(),
	)
	return &Dispatcher{inner: d}
}

// Start spawns the PollFetches goroutine. Safe to call once.
func (d *Dispatcher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.inner.Run(ctx)
	}()
}

// Stop signals shutdown and waits for the dispatcher + per-partition workers
// to drain.
func (d *Dispatcher) Stop() error {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
	return nil
}

// decodeRecord unmarshals the protobuf Row payload on a Kafka record. A decode
// failure is logged once and the record is dropped — malformed payloads are
// not retriable and would poison the partition otherwise.
func decodeRecord(r *kgo.Record) (*Row, error) {
	row := &Row{}
	if err := proto.Unmarshal(r.Value, row); err != nil {
		slog.Warn("logs dispatcher: unmarshal dropped one record", slog.Any("error", err))
		return nil, err
	}
	return row, nil
}
