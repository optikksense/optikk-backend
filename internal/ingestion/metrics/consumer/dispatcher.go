package consumer

import (
	"context"
	"encoding/base64"
	"log/slog"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/dlq"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Dispatcher is the metrics-signal entrypoint for the generic ingest
// pipeline. Migrated from the legacy single-goroutine consumer.go: gains
// per-partition workers, Pause/Resume backpressure, retry + DLQ, and the
// same Prometheus instrumentation as logs/spans.
type Dispatcher struct {
	inner *ingest.Dispatcher[*schema.Row]

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewDispatcher wires the shared generic dispatcher around the metrics-
// specific decoder, worker factory, CH batch insert, and DLQ sink.
func NewDispatcher(
	kafka *kconsumer.Consumer,
	ch clickhouse.Conn,
	dlqP *dlq.Producer,
	pc config.IngestPipelineConfig,
) *Dispatcher {
	writer := NewWriter(ch, dlqP, pc)
	factory := func() *ingest.Worker[*schema.Row] {
		return newMetricsWorker(writer, kafka, pc)
	}
	d := ingest.NewDispatcher[*schema.Row](
		"metrics",
		kafka.Client(),
		decodeRecord,
		factory,
		ingest.DispatcherOptsFromPipeline(pc),
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

// decodeRecord unmarshals the protobuf Row payload on a Kafka record. A
// decode failure is logged once and the record is dropped — malformed
// payloads are not retriable.
func decodeRecord(r *kgo.Record) (*schema.Row, error) {
	row := &schema.Row{}
	if err := proto.Unmarshal(r.Value, row); err != nil {
		slog.Warn("metrics dispatcher: unmarshal dropped one record",
			slog.Any("error", err),
			slog.String("raw_base64", base64.StdEncoding.EncodeToString(r.Value)))
		return nil, err
	}
	return row, nil
}
