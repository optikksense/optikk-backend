package logs

import (
	"context"
	"fmt"
	"strconv"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer owns the Kafka topic + serialization details for the log signal.
// The Kafka transport itself is supplied by infra/kafka.Producer so this file
// stays under the 200 LOC cap and all cross-signal transport knobs live in
// exactly one place.
type Producer struct {
	kafka *kafkainfra.Producer
	topic string
}

func NewProducer(kafka *kafkainfra.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkainfra.IngestTopic(topicPrefix, kafkainfra.SignalLogs),
	}
}

// Publish marshals every row once and hands the whole slice to the Kafka
// producer's PublishBatch (async Produce + WaitGroup) so the handler does not
// pay one network round-trip per row. Partitioning keys on team_id so all
// rows for one team land on the same partition (preserves per-team ordering).
func (p *Producer) Publish(ctx context.Context, rows []*Row) error {
	if len(rows) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("logs producer: marshal: %w", err)
		}
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
