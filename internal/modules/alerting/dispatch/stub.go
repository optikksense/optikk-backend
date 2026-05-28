package dispatch

import (
	"context"
	"log/slog"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// Stub is the no-op transport used for channel types we ship as "Install"
// cards in v1 (pagerduty / opsgenie / teams / email / webhook / jira). It
// logs the payload at info level so test deliveries are visible in dev logs,
// then succeeds — the channels repo marks the channel as delivered.
type Stub struct{}

func NewStub() *Stub { return &Stub{} }

func (s *Stub) Send(ctx context.Context, ch models.ChannelRow, p Payload) error {
	slog.InfoContext(ctx, "alerting: stub transport delivery (not wired)",
		slog.String("channel_type", ch.Type),
		slog.String("channel_name", ch.Name),
		slog.String("transition", p.Transition),
		slog.String("monitor", p.MonitorName),
		slog.Float64("value", p.Value),
		slog.Float64("threshold", p.Threshold),
	)
	return nil
}
