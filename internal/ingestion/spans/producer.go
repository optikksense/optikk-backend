package spans

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// ProducerConfig declares the topic-side identity for the spans signal.
// Partitions / Replicas / RetentionHours feed kafka.EnsureTopics at boot.
type ProducerConfig struct {
	Topic          string
	Partitions     int32
	Replicas       int16
	RetentionHours int
}

// Producer turns mapped Rows into Kafka records and publishes them via the
// shared *kafkainfra.Producer. One record per Row; key is teamID for sticky
// per-team partitioning. Producer-side batching is delegated to franz-go.
type Producer struct {
	cfg  ProducerConfig
	base *kafkainfra.Producer
}

func NewProducer(cfg ProducerConfig, base *kafkainfra.Producer) *Producer {
	return &Producer{cfg: cfg, base: base}
}

// Publish marshals every row into one kgo.Record and produces them in one
// async batch (PublishBatch waits for all acks before returning). The OTLP
// gRPC handler blocks on this until the broker has the records.
func (p *Producer) Publish(ctx context.Context, rows []*schema.Row) error {
	if len(rows) == 0 {
		return nil
	}
	now := time.Now()
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("spans producer: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic:     p.cfg.Topic,
			Key:       []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value:     value,
			Timestamp: now,
		})
	}
	if err := p.base.PublishBatch(ctx, records); err != nil {
		return fmt.Errorf("spans producer: publish batch: %w", err)
	}
	return nil
}
