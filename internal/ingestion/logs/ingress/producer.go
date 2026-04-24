// Package ingress is the write-side of the logs signal: the gRPC handler
// accepts OTLP requests and hands mapped rows to the Kafka Producer declared
// here. The consumer side lives in sibling subpackages.
package ingress

import (
	"context"
	"fmt"
	"strconv"

	kafkaproducer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/producer"
	kafkatopics "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/topics"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer owns the Kafka topic + serialization details for the log signal.
// The Kafka transport itself is supplied by infra/kafka/producer.Producer so
// this file stays small and all cross-signal transport knobs live in exactly
// one place.
type Producer struct {
	kafka *kafkaproducer.Producer
	topic string
}

func NewProducer(kafka *kafkaproducer.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkatopics.IngestTopic(topicPrefix, kafkatopics.SignalLogs),
	}
}

// Publish marshals every row and hands the whole slice to the Kafka
// producer's PublishBatch (async Produce + WaitGroup) so the handler does not
// pay one network round-trip per row. Partitioning keys on team_id so all
// rows for one team land on the same partition (preserves per-team ordering).
//
// The protobuf marshal uses a pooled scratch buffer via
// proto.MarshalOptions.MarshalAppend so the backing array is reused across
// rows in the batch. Each kgo.Record.Value is a fresh copy — franz-go retains
// the slice until ack, so we cannot share it with the pool after return.
func (p *Producer) Publish(ctx context.Context, rows []*schema.Row) error {
	if len(rows) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(rows))
	scratch := ingest.AcquireMarshalBuffer()
	defer ingest.ReleaseMarshalBuffer(scratch)
	opts := proto.MarshalOptions{}
	for _, r := range rows {
		out, err := opts.MarshalAppend((*scratch)[:0], r)
		if err != nil {
			return fmt.Errorf("logs producer: marshal: %w", err)
		}
		*scratch = out
		// Copy into the record so the pooled buffer can be reused for the
		// next row; franz-go holds onto Value until the broker acks.
		value := make([]byte, len(out))
		copy(value, out)
		records = append(records, &kgo.Record{
			Topic: p.topic,
			Key:   []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value: value,
		})
	}
	if err := p.kafka.PublishBatch(ctx, records); err != nil {
		return fmt.Errorf("logs producer: publish batch: %w", err)
	}
	return nil
}
