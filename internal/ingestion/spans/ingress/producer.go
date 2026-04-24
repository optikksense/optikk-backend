// Package ingress is the write-side of the spans signal: the gRPC handler
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
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer owns the Kafka topic + serialization for the span signal. The
// Kafka transport itself is supplied by infra/kafka/producer.Producer.
type Producer struct {
	kafka *kafkaproducer.Producer
	topic string
}

func NewProducer(kafka *kafkaproducer.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkatopics.IngestTopic(topicPrefix, kafkatopics.SignalSpans),
	}
}

// Publish batches every row into one async Produce call so the OTLP handler
// pays one round-trip for the whole request instead of one per row.
// Partitioning keys on team_id to preserve per-team ordering.
//
// Marshal reuses a pooled scratch buffer via MarshalAppend; each record's
// value is copied out so franz-go can retain the bytes until the broker acks.
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
			return fmt.Errorf("spans producer: marshal: %w", err)
		}
		*scratch = out
		value := make([]byte, len(out))
		copy(value, out)
		records = append(records, &kgo.Record{
			Topic: p.topic,
			Key:   []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value: value,
		})
	}
	if err := p.kafka.PublishBatch(ctx, records); err != nil {
		return fmt.Errorf("spans producer: publish batch: %w", err)
	}
	return nil
}
