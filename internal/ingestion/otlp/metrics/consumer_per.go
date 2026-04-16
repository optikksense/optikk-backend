package metrics

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PersistenceConsumer consumes metrics from Kafka and flushes them to ClickHouse.
type PersistenceConsumer struct {
	runner *kafka.ConsumerRunner[*proto.MetricRow]
}

func NewPersistenceConsumer(client *kgo.Client, flusher *otlp.CHFlusher[*MetricRow]) *PersistenceConsumer {
	c := &PersistenceConsumer{}
	c.runner = &kafka.ConsumerRunner[*proto.MetricRow]{
		Signal: "metrics-persist",
		Client: client,
		NewMsg: func() *proto.MetricRow { return &proto.MetricRow{} },
		OnRows: func(ctx context.Context, rows []*proto.MetricRow) error {
			chRows := make([]*MetricRow, len(rows))
			for i, r := range rows {
				chRows[i] = FromProto(r)
			}
			return flusher.Flush(chRows)
		},
	}
	return c
}

func (c *PersistenceConsumer) Start(ctx context.Context) { c.runner.Start(ctx) }
func (c *PersistenceConsumer) Stop() error             { return c.runner.Stop() }
