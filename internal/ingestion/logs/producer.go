package logs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/logs/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type ProducerConfig struct {
	Topic          string
	Partitions     int32
	Replicas       int16
	RetentionHours int
}

type Producer struct {
	cfg  ProducerConfig
	base *kafkainfra.Producer
}

func NewProducer(cfg ProducerConfig, base *kafkainfra.Producer) *Producer {
	return &Producer{cfg: cfg, base: base}
}

func (p *Producer) Publish(ctx context.Context, rows []*schema.Row) error {
	if len(rows) == 0 {
		return nil
	}
	now := time.Now()
	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		value, err := proto.Marshal(r)
		if err != nil {
			return fmt.Errorf("logs producer: marshal: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic:     p.cfg.Topic,
			Key:       []byte(strconv.FormatUint(uint64(r.GetTeamId()), 10)),
			Value:     value,
			Timestamp: now,
		})
	}
	if err := p.base.PublishBatch(ctx, records); err != nil {
		return fmt.Errorf("logs producer: publish batch: %w", err)
	}
	return nil
}
