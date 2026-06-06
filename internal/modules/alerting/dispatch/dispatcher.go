package dispatch

import (
	"context"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// DefaultDispatcher routes alert payloads to their configured channels,
// using Slack webhook or a fallback stub.
type DefaultDispatcher struct {
	slack *SlackWebhook
	stub  *Stub
}

func NewDefaultDispatcher() *DefaultDispatcher {
	return &DefaultDispatcher{
		slack: NewSlackWebhook(),
		stub:  NewStub(),
	}
}

func (d *DefaultDispatcher) Dispatch(ctx context.Context, ch models.ChannelRow, p Payload) error {
	if ch.Type == "slack" {
		return d.slack.Send(ctx, ch, p)
	}
	return d.stub.Send(ctx, ch, p)
}
