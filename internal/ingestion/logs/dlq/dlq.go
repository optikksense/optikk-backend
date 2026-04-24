// Package dlq publishes exhausted logs batches to the per-signal DLQ Kafka
// topic. Wraps infra/kafka/producer.Producer so the logs ingest + DLQ share
// one underlying kgo.Client.
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
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// dlqLogCooldown throttles the per-publish-failure warn log. A broken DLQ
// broker at 200k rps would otherwise spam stderr faster than stdout can
// render. The metric is authoritative; the log is a human breadcrumb.
const dlqLogCooldown = 10 * time.Second

// Producer writes exhausted batches to the per-signal DLQ topic. It wraps
// the shared kafka/producer.Producer so one underlying kgo.Client services
// both the ingest and DLQ topics.
type Producer struct {
	kafka *kafkaproducer.Producer
	topic string

	// lastErrLogUnixNs is the monotonic timestamp of the last stderr warn
	// emitted for a failed DLQ publish. CompareAndSwap gates subsequent
	// warns within dlqLogCooldown.
	lastErrLogUnixNs atomic.Int64
}

// NewProducer derives the DLQ topic name from the ingest prefix (e.g.
// "optikk.ingest" → "optikk.dlq.logs") using the topics-package helper.
func NewProducer(kafka *kafkaproducer.Producer, topicPrefix string) *Producer {
	return &Producer{
		kafka: kafka,
		topic: kafkatopics.DLQTopic(topicPrefix, kafkatopics.SignalLogs),
	}
}

// Publish serialises every row + the failure reason into one Kafka record per
// row and produces them in a single async batch. The reason is attached as a
// header so downstream tooling can filter / re-route without reparsing the
// payload. DLQ publish failures bump writer_dlq_publish_failed_total and
// emit a rate-limited warn; they never propagate into the consumer loop.
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
		ingest.DLQPublishFailed.WithLabelValues("logs").Inc()
		return
	}
	if err := d.kafka.PublishBatch(ctx, records); err != nil {
		d.logRateLimited(ctx, err)
		ingest.DLQPublishFailed.WithLabelValues("logs").Inc()
	}
}

func (d *Producer) buildRecords(rows []*schema.Row, reasonStr string) ([]*kgo.Record, error) {
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return nil, fmt.Errorf("logs dlq: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: d.topic,
			Key:   []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value: value,
			Headers: []kgo.RecordHeader{
				{Key: "x-dlq-reason", Value: []byte(reasonStr)},
				{Key: "x-dlq-signal", Value: []byte(kafkatopics.SignalLogs)},
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
	slog.WarnContext(ctx, "logs dlq: publish failed (rate-limited log)",
		slog.Any("error", err))
}
