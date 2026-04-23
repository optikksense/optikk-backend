package logs

import (
	"context"
	"fmt"
	"strconv"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// DLQProducer writes exhausted batches to the per-signal DLQ topic. It wraps
// the shared kafkainfra.Producer so one underlying kgo.Client services both
// the ingest and DLQ topics.
type DLQProducer struct {
	kafka *kafkainfra.Producer
	topic string
}

// NewDLQProducer derives the DLQ topic name from the ingest prefix (e.g.
// "optikk.ingest" → "optikk.dlq.logs") using the topics-package helper.
func NewDLQProducer(kafka *kafkainfra.Producer, topicPrefix string) *DLQProducer {
	return &DLQProducer{
		kafka: kafka,
		topic: kafkainfra.DLQTopic(topicPrefix, kafkainfra.SignalLogs),
	}
}

// Publish serialises every row + the failure reason into one Kafka record per
// row and produces them in a single async batch. The reason is attached as a
// header so downstream tooling can filter / re-route without reparsing the
// payload. Errors from the DLQ itself are returned so the writer can log;
// they never bubble back into the consumer group.
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
			return fmt.Errorf("logs dlq: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: d.topic,
			Key:   []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "x-dlq-reason", Value: []byte(reasonStr)},
				{Key: "x-dlq-signal", Value: []byte(kafkainfra.SignalLogs)},
			},
		})
	}
	return d.kafka.PublishBatch(ctx, records)
}
