package spans

import (
	"context"
	"fmt"
	"strconv"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
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

// Publish serialises each row as protobuf and produces it to the span ingest
// topic. Keying on team_id preserves ordering per team.
func (p *Producer) Publish(ctx context.Context, rows []*Row) error {
	if len(rows) == 0 {
		return nil
	}
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("spans producer: marshal: %w", err)
		}
		key := []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10))
		if err := p.kafka.Produce(ctx, p.topic, key, value); err != nil {
			return fmt.Errorf("spans producer: produce: %w", err)
		}
	}
	return nil
}
