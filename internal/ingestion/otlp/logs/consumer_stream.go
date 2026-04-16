package logs

import (
	"context"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StreamingConsumer consumes logs from Kafka and publishes them to the LiveTail hub.
type StreamingConsumer struct {
	runner *kafka.ConsumerRunner[*proto.LogRow]
}

func NewStreamingConsumer(client *kgo.Client, hub livetail.Hub) *StreamingConsumer {
	c := &StreamingConsumer{}
	c.runner = &kafka.ConsumerRunner[*proto.LogRow]{
		Signal: "logs-stream",
		Client: client,
		NewMsg: func() *proto.LogRow { return &proto.LogRow{} },
		OnRows: func(ctx context.Context, rows []*proto.LogRow) error {
			for _, r := range rows {
				chRow := FromProto(r)
				if payload, ok := LiveTailStreamPayload(chRow); ok && payload != nil {
					hub.Publish(int64(r.TeamID), payload)
				}
			}
			return nil
		},
	}
	return c
}

func (c *StreamingConsumer) Start(ctx context.Context) { c.runner.Start(ctx) }
func (c *StreamingConsumer) Stop() error             { return c.runner.Stop() }
