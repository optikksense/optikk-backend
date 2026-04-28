package spans

import (
	"context"
	"log/slog"

	kafkainfra "github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/spans/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// ConsumerConfig declares this signal's consumer identity.
type ConsumerConfig struct {
	Topic         string
	ConsumerGroup string
}

// Consumer polls Kafka, decodes records, hands them to the writer, and on
// failure routes the original record bytes to the DLQ topic. Offsets are
// committed by the underlying *kafkainfra.Consumer once the handler returns
// nil.
type Consumer struct {
	cfg    ConsumerConfig
	client *kafkainfra.Consumer
	writer *Writer
	dlq    *DLQ
}

func NewConsumer(cfg ConsumerConfig, client *kafkainfra.Consumer, w *Writer, dlq *DLQ) *Consumer {
	return &Consumer{cfg: cfg, client: client, writer: w, dlq: dlq}
}

func (c *Consumer) Run(ctx context.Context) {
	c.client.Run(ctx, c.handle)
}

func (c *Consumer) handle(ctx context.Context, recs []*kgo.Record) error {
	rows := make([]*schema.Row, 0, len(recs))
	for _, r := range recs {
		row := &schema.Row{}
		if err := proto.Unmarshal(r.Value, row); err != nil {
			slog.WarnContext(ctx, "spans consumer: dropped malformed record",
				slog.Int("partition", int(r.Partition)),
				slog.Int64("offset", r.Offset),
				slog.Any("error", err),
			)
			continue
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil
	}
	if err := c.writer.Insert(ctx, rows); err != nil {
		slog.ErrorContext(ctx, "spans consumer: CH insert failed → DLQ",
			slog.Int("rows", len(rows)),
			slog.Any("error", err),
		)
		c.dlq.PublishAll(ctx, recs, err)
		return nil // commit offsets to unblock the partition
	}
	return nil
}
