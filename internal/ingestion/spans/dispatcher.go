package spans

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/indexer"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Dispatcher is the spans-signal entrypoint for the generic ingest pipeline.
// Mirrors logs/Dispatcher — PollFetches loop + per-partition worker pool
// with backpressure via Pause/Resume. Drops malformed protobuf records.
type Dispatcher struct {
	inner *ingest.Dispatcher[*Row]

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewDispatcher(consumer *kafkainfra.Consumer, ch clickhouse.Conn, dlq *DLQProducer, asm *indexer.Assembler) *Dispatcher {
	writer := NewWriter(ch, dlq, asm)
	factory := func() *ingest.Worker[*Row] {
		return newSpansWorker(writer, consumer)
	}
	d := ingest.NewDispatcher[*Row](
		"spans",
		consumer.Client(),
		decodeRecord,
		factory,
		ingest.DefaultDispatcherOptions(),
	)
	return &Dispatcher{inner: d}
}

func (d *Dispatcher) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.inner.Run(ctx)
	}()
}

func (d *Dispatcher) Stop() error {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
	return nil
}

func decodeRecord(r *kgo.Record) (*Row, error) {
	row := &Row{}
	if err := proto.Unmarshal(r.Value, row); err != nil {
		slog.Warn("spans dispatcher: unmarshal dropped one record", slog.Any("error", err))
		return nil, err
	}
	return row, nil
}
