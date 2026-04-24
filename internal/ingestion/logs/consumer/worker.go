// Package consumer drives the Kafka → ClickHouse half of the logs signal.
// Dispatcher polls partitions and fans records out to per-partition workers;
// the writer batches into CH with retry + DLQ. Sibling schema/ carries the
// wire format the dispatcher decodes into.
package consumer

import (
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
)

// newLogsWorker composes the shared ingest.Worker[*schema.Row] generic with
// the logs-specific size function and CH-backed writer. Called once per
// (topic, partition) pair by the dispatcher factory. Queue size + accumulator
// triggers come from the per-signal IngestPipelineConfig.
func newLogsWorker(
	writer *ingest.Writer[*schema.Row],
	kafka *kconsumer.Consumer,
	pc config.IngestPipelineConfig,
) *ingest.Worker[*schema.Row] {
	return ingest.NewWorker[*schema.Row](
		"logs",
		pc.WorkerQueueSize,
		writer,
		ingest.AccumulatorCfgFromPipeline(pc),
		logsItemSize,
		kafka.Client(),
	)
}

// logsItemSize feeds the bytes-trigger accumulator. The hot path always
// populates Raw via the dispatcher decode step, so we just read the record
// payload length. No test-only fallback — benchmarks populate Raw directly.
func logsItemSize(it ingest.Item[*schema.Row]) int {
	if it.Raw == nil {
		return 0
	}
	return len(it.Raw.Value)
}
