package logs

import (
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"google.golang.org/protobuf/proto"
)

// workerQueueSize bounds the per-partition channel. Paired with the
// dispatcher's PauseDepth/ResumeDepth thresholds (80% / 40%) to back-pressure
// OTLP producers before this channel blocks the PollFetches loop.
const workerQueueSize = 1024

// newLogsWorker composes the shared ingest.Worker[*Row] generic with the
// logs-specific size function and CH-backed writer. Called once per
// (topic, partition) pair by the dispatcher factory.
func newLogsWorker(writer *ingest.Writer[*Row], consumer *kafkainfra.Consumer) *ingest.Worker[*Row] {
	return ingest.NewWorker[*Row](
		"logs",
		workerQueueSize,
		writer,
		ingest.DefaultAccumulatorConfig(),
		logsItemSize,
		consumer.Client(),
	)
}

// logsItemSize feeds the bytes-trigger accumulator. Uses the raw Kafka record
// length when available (hot path); falls back to a re-marshal estimate in
// tests where Raw may be nil.
func logsItemSize(it ingest.Item[*Row]) int {
	if it.Raw != nil {
		return len(it.Raw.Value)
	}
	if it.Payload == nil {
		return 0
	}
	b, err := proto.Marshal(it.Payload)
	if err != nil {
		return 0
	}
	return len(b)
}
