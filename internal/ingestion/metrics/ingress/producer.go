// Package ingress is the write-side of the metrics signal: the gRPC handler
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
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer owns the Kafka topic + serialization for the metric signal. The
// Kafka transport itself is supplied by infra/kafka/producer.Producer.
type Producer struct {
	kafka *kafkaproducer.Producer
	topic string
}

func NewProducer(kafka *kafkaproducer.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkatopics.IngestTopic(topicPrefix, kafkatopics.SignalMetrics),
	}
}

// Publish batches every row into one async PublishBatch call so the OTLP
// handler pays one round-trip per request instead of per record. Partition
// key on team_id preserves per-team ordering. Marshal reuses a pooled
// scratch buffer; record.Value is copied out because franz-go retains it
// until ack.
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
			return fmt.Errorf("metrics producer: marshal: %w", err)
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
		return fmt.Errorf("metrics producer: publish batch: %w", err)
	}
	return nil
}
