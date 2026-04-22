package spans

import (
	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"google.golang.org/protobuf/proto"
)

const workerQueueSize = 1024

// newSpansWorker composes the shared ingest.Worker[*Row] with the
// spans-specific size function and CH-backed writer. Called once per
// (topic, partition) pair by the dispatcher factory.
func newSpansWorker(writer *ingest.Writer[*Row], consumer *kafkainfra.Consumer) *ingest.Worker[*Row] {
	return ingest.NewWorker[*Row](
		"spans",
		workerQueueSize,
		writer,
		ingest.DefaultAccumulatorConfig(),
		spansItemSize,
		consumer.Client(),
	)
}

// spansItemSize feeds the bytes-trigger accumulator. Hot path uses the
// raw Kafka record length; test-only path falls back to re-marshal.
func spansItemSize(it ingest.Item[*Row]) int {
	if it.Raw != nil {
		return len(it.Raw.Value)
	}
	if it.Payload == nil {
		return 0
	}
	return proto.Size(it.Payload)
}
