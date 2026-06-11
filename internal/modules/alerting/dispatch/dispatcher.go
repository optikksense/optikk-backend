package dispatch

import (
	"context"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// Dispatcher routes alert payloads to their configured channels,
// using Slack webhook or a fallback stub.
type Dispatcher struct {
	slack *SlackWebhook
	stub  *Stub
}

func NewDefaultDispatcher() *Dispatcher {
	return &Dispatcher{
		slack: NewSlackWebhook(),
		stub:  NewStub(),
	}
}

func (d *Dispatcher) Dispatch(ctx context.Context, ch models.ChannelRow, p Payload) error {
	if ch.Type == "slack" {
		return d.slack.Send(ctx, ch, p)
	}
	return d.stub.Send(ctx, ch, p)
}
