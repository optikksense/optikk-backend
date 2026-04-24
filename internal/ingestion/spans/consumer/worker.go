// Package consumer drives the Kafka → ClickHouse half of the spans signal.
// Dispatcher polls partitions and fans records out to per-partition workers;
// the writer batches into CH with retry + DLQ and feeds the trace-assembly
// indexer once each insert commits.
package consumer

import (
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
)

// newSpansWorker composes the shared ingest.Worker[*schema.Row] with the
// spans-specific size function and CH-backed writer. Called once per
// (topic, partition) pair by the dispatcher factory. Queue size + accumulator
// triggers come from the per-signal IngestPipelineConfig.
func newSpansWorker(
	writer *ingest.Writer[*schema.Row],
	kafka *kconsumer.Consumer,
	pc config.IngestPipelineConfig,
) *ingest.Worker[*schema.Row] {
	return ingest.NewWorker[*schema.Row](
		"spans",
		pc.WorkerQueueSize,
		writer,
		ingest.AccumulatorCfgFromPipeline(pc),
		spansItemSize,
		kafka.Client(),
	)
}

// spansItemSize feeds the bytes-trigger accumulator. Hot path always has Raw
// populated by the dispatcher decode step; benchmarks likewise construct
// Items with Raw. Nil Raw → 0 bytes.
func spansItemSize(it ingest.Item[*schema.Row]) int {
	if it.Raw == nil {
		return 0
	}
	return len(it.Raw.Value)
}
