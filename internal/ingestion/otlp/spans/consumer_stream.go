package spans

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/kafka"
	"github.com/Optikk-Org/optikk-backend/internal/ingestion/proto"
	"github.com/Optikk-Org/optikk-backend/internal/modules/livetail"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StreamingConsumer consumes spans from Kafka and publishes them to the LiveTail hub.
type StreamingConsumer struct {
	runner *kafka.ConsumerRunner[*proto.SpanRow]
}

func NewStreamingConsumer(client *kgo.Client, hub livetail.Hub) *StreamingConsumer {
	c := &StreamingConsumer{}
	c.runner = &kafka.ConsumerRunner[*proto.SpanRow]{
		Signal: "spans-stream",
		Client: client,
		NewMsg: func() *proto.SpanRow { return &proto.SpanRow{} },
		OnRows: func(ctx context.Context, rows []*proto.SpanRow) error {
			now := time.Now().UnixMilli()
			for _, r := range rows {
				chRow := FromProto(r)
				data, err := SpanLiveTailStreamPayload(chRow, now)
				if err == nil && data != nil {
					hub.Publish(int64(r.TeamID), data)
				}
			}
			return nil
		},
	}
	return c
}

func (c *StreamingConsumer) Start(ctx context.Context) { c.runner.Start(ctx) }
func (c *StreamingConsumer) Stop() error             { return c.runner.Stop() }
