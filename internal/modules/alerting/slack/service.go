package slack

import (
	"context"
	"log/slog"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/channels"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/engine"
)

// Service covers the Slack-facing surface — outbound TestSlack from the rule
// author UI, plus the inbound action-button callback. Delivery itself flows
// through the shared engine.Dispatcher which owns the Slack channel client.
type Service interface {
	TestSlack(ctx context.Context, req SlackTestRequest) (*SlackTestResponse, error)
	HandleCallback(ctx context.Context, payload map[string]any) error
}

type service struct {
	dispatcher *engine.Dispatcher
}

func NewService(dispatcher *engine.Dispatcher) Service {
	return &service{dispatcher: dispatcher}
}

func (s *service) TestSlack(ctx context.Context, req SlackTestRequest) (*SlackTestResponse, error) {
	def := shared.NormalizeAlertRuleDefinition(req.Rule)
	if err := shared.ValidateAlertRuleDefinition(def); err != nil {
		return nil, err
	}
	preview := shared.PreviewNotification(def)
	rendered := channels.Rendered{
		Title:       preview.Title,
		Body:        preview.Body,
		Severity:    def.Condition.Severity,
		DeepLinkURL: "",
	}
	if s.dispatcher == nil {
		return &SlackTestResponse{Delivered: false, Error: "slack dispatcher unavailable", Notification: preview}, nil
	}
	if err := s.dispatcher.SendSlack(ctx, def.Delivery.SlackWebhookURL, rendered); err != nil {
		return &SlackTestResponse{Delivered: false, Error: err.Error(), Notification: preview}, nil
	}
	return &SlackTestResponse{Delivered: true, Notification: preview}, nil
}

// HandleCallback is the inbound Slack interactivity route. v1 logs + accepts;
// a signed callback implementation requires sharing a secret and parsing the
// envelope, tracked as a follow-up.
func (s *service) HandleCallback(_ context.Context, payload map[string]any) error {
	slog.Info("alerting: slack callback received", slog.Any("payload", payload))
	return nil
}
