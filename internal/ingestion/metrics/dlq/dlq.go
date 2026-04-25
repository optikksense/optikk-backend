// Package dlq publishes exhausted metrics batches to the per-signal DLQ Kafka
// topic (optikk.dlq.metrics). Mirrors logs + spans DLQ shape so downstream
// tooling can treat the three topics uniformly.
package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	kafkaproducer "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/producer"
	kafkatopics "github.com/Optikk-Org/optikk-backend/internal/infra/kafka/topics"
	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka/ingest"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/metrics/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const dlqLogCooldown = 10 * time.Second

// Producer writes exhausted batches to the metrics DLQ topic. Wraps the
// shared kafka/producer.Producer so one underlying kgo.Client services both
// ingest and DLQ topics.
type Producer struct {
	kafka *kafkaproducer.Producer
	topic string

	lastErrLogUnixNs atomic.Int64
}

func NewProducer(kafka *kafkaproducer.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkatopics.DLQTopic(topicPrefix, kafkatopics.SignalMetrics),
	}
}

// Publish serialises every row + the failure reason into one Kafka record per
// row and produces them in a single async batch. Errors bump
// writer_dlq_publish_failed_total and emit a rate-limited warn; they never
// propagate into the consumer loop.
func (d *Producer) Publish(ctx context.Context, rows []*schema.Row, reason error) {
	if d == nil || len(rows) == 0 {
		return
	}
	reasonStr := ""
	if reason != nil {
		reasonStr = reason.Error()
	}
	records, err := d.buildRecords(rows, reasonStr)
	if err != nil {
		d.logRateLimited(ctx, err)
		ingest.DLQPublishFailed.WithLabelValues("metrics").Inc()
		return
	}
	if err := d.kafka.PublishBatch(ctx, records); err != nil {
		d.logRateLimited(ctx, err)
		ingest.DLQPublishFailed.WithLabelValues("metrics").Inc()
	}
}

func (d *Producer) buildRecords(rows []*schema.Row, reasonStr string) ([]*kgo.Record, error) {
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return nil, fmt.Errorf("metrics dlq: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: d.topic,
			Key:   []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "x-dlq-reason", Value: []byte(reasonStr)},
				{Key: "x-dlq-signal", Value: []byte(kafkatopics.SignalMetrics)},
			},
		})
	}
	return records, nil
}

func (d *Producer) logRateLimited(ctx context.Context, err error) {
	now := time.Now().UnixNano()
	last := d.lastErrLogUnixNs.Load()
	if now-last < int64(dlqLogCooldown) {
		return
	}
	if !d.lastErrLogUnixNs.CompareAndSwap(last, now) {
		return
	}
	slog.WarnContext(ctx, "metrics dlq: publish failed (rate-limited log)",
		slog.Any("error", err))
}
