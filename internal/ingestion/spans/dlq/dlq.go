// Package dlq publishes exhausted spans batches to the per-signal DLQ Kafka
// topic (optikk.dlq.spans). Mirrors the logs + metrics DLQ shape so downstream
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
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// dlqLogCooldown throttles the per-publish-failure warn log. See
// logs/dlq/dlq.go for the rationale; mirrored here for the spans signal.
const dlqLogCooldown = 10 * time.Second

// Producer writes exhausted batches to the spans DLQ topic. Wraps the shared
// kafka/producer.Producer so one underlying kgo.Client services both ingest
// and DLQ topics.
type Producer struct {
	kafka *kafkaproducer.Producer
	topic string

	lastErrLogUnixNs atomic.Int64
}

func NewProducer(kafka *kafkaproducer.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkatopics.DLQTopic(topicPrefix, kafkatopics.SignalSpans),
	}
}

// Publish serialises every row + the failure reason into one Kafka record per
// row (reason attached as header) and produces them in one async batch.
// Publish errors bump writer_dlq_publish_failed_total and emit a rate-limited
// warn; they never propagate into the consumer loop.
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
		ingest.DLQPublishFailed.WithLabelValues("spans").Inc()
		return
	}
	if err := d.kafka.PublishBatch(ctx, records); err != nil {
		d.logRateLimited(ctx, err)
		ingest.DLQPublishFailed.WithLabelValues("spans").Inc()
	}
}

func (d *Producer) buildRecords(rows []*schema.Row, reasonStr string) ([]*kgo.Record, error) {
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return nil, fmt.Errorf("spans dlq: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: d.topic,
			Key:   []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "x-dlq-reason", Value: []byte(reasonStr)},
				{Key: "x-dlq-signal", Value: []byte(kafkatopics.SignalSpans)},
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
	slog.WarnContext(ctx, "spans dlq: publish failed (rate-limited log)",
		slog.Any("error", err))
}
