package consumer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/dlq"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Dispatcher is the spans-signal entrypoint for the generic ingest pipeline.
// Mirrors logs/consumer.Dispatcher — PollFetches loop + per-partition worker
// pool with backpressure via Pause/Resume. Drops malformed protobuf records.
type Dispatcher struct {
	inner *ingest.Dispatcher[*schema.Row]

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewDispatcher(
	kafka *kconsumer.Consumer,
	ch clickhouse.Conn,
	dlqP *dlq.Producer,
	pc config.IngestPipelineConfig,
) *Dispatcher {
	writer := NewWriter(ch, dlqP, pc)
	factory := func() *ingest.Worker[*schema.Row] {
		return newSpansWorker(writer, kafka, pc)
	}
	d := ingest.NewDispatcher[*schema.Row](
		"spans",
		kafka.Client(),
		decodeRecord,
		factory,
		ingest.DispatcherOptsFromPipeline(pc),
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

func decodeRecord(r *kgo.Record) (*schema.Row, error) {
	row := &schema.Row{}
	if err := proto.Unmarshal(r.Value, row); err != nil {
		slog.Warn("spans dispatcher: unmarshal dropped one record", slog.Any("error", err))
		return nil, err
	}
	return row, nil
}
