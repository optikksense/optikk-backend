// Package consumer drives the Kafka → ClickHouse half of the metrics signal.
// Dispatcher polls partitions and fans records out to per-partition workers;
// the writer batches into CH with retry + DLQ.
package consumer

import (
	"github.com/Optikk-Org/optikk-backend/internal/config"
	kconsumer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/consumer"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
)

// newMetricsWorker composes the shared ingest.Worker[*schema.Row] generic
// with the metrics-specific size function and CH-backed writer. Called once
// per (topic, partition) pair by the dispatcher factory.
func newMetricsWorker(
	writer *ingest.Writer[*schema.Row],
	kafka *kconsumer.Consumer,
	pc config.IngestPipelineConfig,
) *ingest.Worker[*schema.Row] {
	return ingest.NewWorker[*schema.Row](
		"metrics",
		pc.WorkerQueueSize,
		writer,
		ingest.AccumulatorCfgFromPipeline(pc),
		metricsItemSize,
		kafka.Client(),
	)
}

// metricsItemSize feeds the bytes-trigger accumulator. Nil Raw → 0 bytes.
func metricsItemSize(it ingest.Item[*schema.Row]) int {
	if it.Raw == nil {
		return 0
	}
	return len(it.Raw.Value)
}
