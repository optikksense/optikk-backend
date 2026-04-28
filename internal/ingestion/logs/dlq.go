package logs

import (
	"context"
	"log/slog"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type DLQ struct {
	base  *kafkainfra.Producer
	topic string
}

func NewDLQ(base *kafkainfra.Producer, topic string) *DLQ {
	return &DLQ{base: base, topic: topic}
}

func (d *DLQ) PublishAll(ctx context.Context, recs []*kgo.Record, reason error) {
	if d == nil || len(recs) == 0 {
		return
	}
	reasonStr := ""
	if reason != nil {
		reasonStr = reason.Error()
	}
	out := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		out = append(out, &kgo.Record{
			Topic: d.topic,
			Key:   r.Key,
			Value: r.Value,
			Headers: []kgo.RecordHeader{
				{Key: "x-dlq-reason", Value: []byte(reasonStr)},
				{Key: "x-dlq-signal", Value: []byte(kafkainfra.SignalLogs)},
			},
		})
	}
	if err := d.base.PublishBatch(ctx, out); err != nil {
		slog.WarnContext(ctx, "logs dlq: publish failed",
			slog.String("topic", d.topic),
			slog.Int("records", len(out)),
			slog.Any("error", err),
		)
	}
}
