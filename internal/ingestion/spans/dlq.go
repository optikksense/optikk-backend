package spans

import (
	"context"
	"fmt"
	"strconv"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// DLQProducer writes exhausted batches to the spans DLQ topic
// (optikk.dlq.spans). Wraps the shared kafkainfra.Producer so one
// underlying kgo.Client services both ingest and DLQ topics.
type DLQProducer struct {
	kafka *kafkainfra.Producer
	topic string
}

func NewDLQProducer(kafka *kafkainfra.Producer, topicPrefix string) *DLQProducer {
	return &DLQProducer{
		kafka: kafka,
		topic: kafkainfra.DLQTopic(topicPrefix, kafkainfra.SignalSpans),
	}
}

// Publish serialises every row + the failure reason into one Kafka record per
// row (reason attached as header) and produces them in one async batch.
func (d *DLQProducer) Publish(ctx context.Context, rows []*Row, reason error) error {
	if d == nil || len(rows) == 0 {
		return nil
	}
	reasonStr := ""
	if reason != nil {
		reasonStr = reason.Error()
	}
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("spans dlq: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: d.topic,
			Key:   []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "x-dlq-reason", Value: []byte(reasonStr)},
				{Key: "x-dlq-signal", Value: []byte(kafkainfra.SignalSpans)},
			},
		})
	}
	return d.kafka.PublishBatch(ctx, records)
}
