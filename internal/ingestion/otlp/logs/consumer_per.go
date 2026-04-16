package logs

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PersistenceConsumer consumes logs from Kafka and flushes them to ClickHouse.
type PersistenceConsumer struct {
	runner *kafka.ConsumerRunner[*proto.LogRow]
}

func NewPersistenceConsumer(client *kgo.Client, flusher *dbutil.CHFlusher[*LogRow]) *PersistenceConsumer {
	c := &PersistenceConsumer{}
	c.runner = &kafka.ConsumerRunner[*proto.LogRow]{
		Signal: "logs-persist",
		Client: client,
		NewMsg: func() *proto.LogRow { return &proto.LogRow{} },
		OnRows: func(ctx context.Context, rows []*proto.LogRow) error {
			chRows := make([]*LogRow, len(rows))
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
