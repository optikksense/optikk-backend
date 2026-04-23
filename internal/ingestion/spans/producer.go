package spans

import (
	"context"
	"fmt"
	"strconv"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer owns the Kafka topic + serialization for the span signal. The
// Kafka transport itself is supplied by infra/kafka.Producer.
type Producer struct {
	kafka *kafkainfra.Producer
	topic string
}

func NewProducer(kafka *kafkainfra.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalSpans),
	}
}

// Publish batches every row into one async Produce call so the OTLP handler
// pays one round-trip for the whole request instead of one per row.
// Partitioning keys on team_id to preserve per-team ordering.
func (p *Producer) Publish(ctx context.Context, rows []*Row) error {
	if len(rows) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("spans producer: marshal: %w", err)
		}
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
