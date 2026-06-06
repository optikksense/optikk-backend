package notifications

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/dispatch"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// Service owns business logic for the four notification sub-resources.
type Service struct {
	repo       Repository
	dispatcher dispatch.Dispatcher
}

func NewService(repo Repository, dispatcher dispatch.Dispatcher) *Service {
	return &Service{repo: repo, dispatcher: dispatcher}
}

// Sentinel errors -----------------------------------------------------------

var (
	ErrNotFound     = errors.New("notification resource not found")
	ErrChannelInUse = errors.New("channel is in use by one or more monitors")
)

// ErrValidation wraps a user-facing validation message.
type ErrValidation struct{ Msg string }

func (e ErrValidation) Error() string { return e.Msg }

// Channels ------------------------------------------------------------------

func (s *Service) CreateChannel(ctx context.Context, teamID int64, req CreateChannelRequest) (ChannelResponse, error) {
	row, err := buildChannelRow(teamID, req)
	if err != nil {
		return ChannelResponse{}, err
	}
	id, err := s.repo.CreateChannel(ctx, row)
	if err != nil {
		return ChannelResponse{}, err
	}
	return s.GetChannel(ctx, teamID, id)
}

func (s *Service) UpdateChannel(ctx context.Context, teamID, id int64, req UpdateChannelRequest) (ChannelResponse, error) {
	row, err := buildChannelRow(teamID, req)
	if err != nil {
		return ChannelResponse{}, err
	}
	if err := s.repo.UpdateChannel(ctx, id, teamID, row); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ChannelResponse{}, ErrNotFound
		}
		return ChannelResponse{}, err
	}
	return s.GetChannel(ctx, teamID, id)
}

func (s *Service) DeleteChannel(ctx context.Context, teamID, id int64) error {
	myRepo, ok := s.repo.(*MySQLRepository)
	if ok {
		inUse, err := myRepo.ChannelInUse(ctx, id, teamID)
		if err != nil {
			return err
		}
		if inUse {
			return ErrChannelInUse
		}
	}
	if err := s.repo.DeleteChannel(ctx, id, teamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

func (s *Service) GetChannel(ctx context.Context, teamID, id int64) (ChannelResponse, error) {
	row, err := s.repo.GetChannel(ctx, id, teamID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ChannelResponse{}, ErrNotFound
		}
		return ChannelResponse{}, err
	}
	usage, _ := s.repo.CountChannelUsage(ctx, teamID)
	return toChannelResponse(row, usage[row.ID]), nil
}

func (s *Service) ListChannels(ctx context.Context, teamID int64) ([]ChannelResponse, error) {
	rows, err := s.repo.ListChannels(ctx, teamID)
	if err != nil {
		return nil, err
	}
	usage, _ := s.repo.CountChannelUsage(ctx, teamID)
	out := make([]ChannelResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, toChannelResponse(row, usage[row.ID]))
	}
	return out, nil
}

// TestChannel runs a synthetic dispatch mirroring a real Alerting trigger.
func (s *Service) TestChannel(ctx context.Context, teamID, id int64) (map[string]any, error) {
	row, err := s.repo.GetChannel(ctx, id, teamID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	payload := dispatch.Payload{
		MonitorID:    0,
		MonitorName:  "[Test] Optikk Monitors delivery",
		MonitorType:  "metric",
		Priority:     "P3",
		Transition:   "ok->alert",
		Status:       "alert",
		Value:        0.42,
		Threshold:    0.05,
		ScopeSummary: "test channel · no real monitor",
		Message:      "If you can see this message, this channel is wired correctly.",
		IsAlert:      true,
	}
	deliverErr := s.dispatcher.Dispatch(ctx, row, payload)
	at := time.Now().UTC()
	errText := sql.NullString{}
	if deliverErr != nil {
		errText = sql.NullString{Valid: true, String: deliverErr.Error()}
	}
	_ = s.repo.MarkChannelDelivered(ctx, id, at, errText)
	out := map[string]any{"ok": deliverErr == nil}
	if deliverErr != nil {
		out["error_text"] = deliverErr.Error()
	}
	return out, nil
}

func buildChannelRow(teamID int64, req CreateChannelRequest) (models.ChannelRow, error) {
	t := strings.TrimSpace(req.Type)
	if !models.IsValidChannelType(t) {
		return models.ChannelRow{}, ErrValidation{Msg: "type must be one of " + strings.Join(models.ChannelTypes, ", ")}
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return models.ChannelRow{}, ErrValidation{Msg: "name is required"}
	}
	cfg := req.Config
	if len(cfg) == 0 {
		cfg = json.RawMessage("{}")
	}
	if t == "slack" {
		var sc models.SlackWebhookConfig
		if err := json.Unmarshal(cfg, &sc); err != nil || strings.TrimSpace(sc.WebhookURL) == "" {
			return models.ChannelRow{}, ErrValidation{Msg: "slack channel requires config.webhook_url"}
		}
	}
	return models.ChannelRow{
		TeamID:     teamID,
		Type:       t,
		Name:       name,
		ConfigJSON: cfg,
	}, nil
}

// Policies ------------------------------------------------------------------

func (s *Service) CreatePolicy(ctx context.Context, teamID int64, req CreatePolicyRequest) (PolicyResponse, error) {
	row, err := buildPolicyRow(teamID, req)
	if err != nil {
		return PolicyResponse{}, err
	}
	id, err := s.repo.CreatePolicy(ctx, row)
	if err != nil {
		return PolicyResponse{}, err
	}
	row.ID = id
	return toPolicyResponse(row), nil
}

func (s *Service) UpdatePolicy(ctx context.Context, teamID, id int64, req UpdatePolicyRequest) (PolicyResponse, error) {
	row, err := buildPolicyRow(teamID, req)
	if err != nil {
		return PolicyResponse{}, err
	}
	row.ID = id
	if err := s.repo.UpdatePolicy(ctx, id, teamID, row); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PolicyResponse{}, ErrNotFound
		}
		return PolicyResponse{}, err
	}
	return toPolicyResponse(row), nil
}

func (s *Service) DeletePolicy(ctx context.Context, teamID, id int64) error {
	if err := s.repo.DeletePolicy(ctx, id, teamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

func (s *Service) ListPolicies(ctx context.Context, teamID int64) ([]PolicyResponse, error) {
	rows, err := s.repo.ListPolicies(ctx, teamID)
	if err != nil {
		return nil, err
	}
	out := make([]PolicyResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, toPolicyResponse(row))
	}
	return out, nil
}

func buildPolicyRow(teamID int64, req CreatePolicyRequest) (models.PolicyRow, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return models.PolicyRow{}, ErrValidation{Msg: "name is required"}
	}
	dsl := strings.TrimSpace(req.MatchDSL)
	if dsl == "" {
		return models.PolicyRow{}, ErrValidation{Msg: "match_dsl is required"}
	}
	actions := req.Actions
	if len(actions) == 0 {
		actions = json.RawMessage("[]")
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	position := 0
	if req.Position != nil {
		position = *req.Position
	}
	return models.PolicyRow{
		TeamID:      teamID,
		Name:        name,
		MatchDSL:    dsl,
		ActionsJSON: actions,
		Enabled:     enabled,
		Position:    position,
	}, nil
}

// Templates -----------------------------------------------------------------

func (s *Service) CreateTemplate(ctx context.Context, teamID int64, req CreateTemplateRequest) (TemplateResponse, error) {
	row, err := buildTemplateRow(teamID, req)
	if err != nil {
		return TemplateResponse{}, err
	}
	id, err := s.repo.CreateTemplate(ctx, row)
	if err != nil {
		return TemplateResponse{}, err
	}
	row.ID = id
	return toTemplateResponse(row), nil
}

func (s *Service) UpdateTemplate(ctx context.Context, teamID, id int64, req UpdateTemplateRequest) (TemplateResponse, error) {
	row, err := buildTemplateRow(teamID, req)
	if err != nil {
		return TemplateResponse{}, err
	}
	row.ID = id
	if err := s.repo.UpdateTemplate(ctx, id, teamID, row); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TemplateResponse{}, ErrNotFound
		}
		return TemplateResponse{}, err
	}
	return toTemplateResponse(row), nil
}

func (s *Service) DeleteTemplate(ctx context.Context, teamID, id int64) error {
	if err := s.repo.DeleteTemplate(ctx, id, teamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

func (s *Service) ListTemplates(ctx context.Context, teamID int64) ([]TemplateResponse, error) {
	rows, err := s.repo.ListTemplates(ctx, teamID)
	if err != nil {
		return nil, err
	}
	out := make([]TemplateResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, toTemplateResponse(row))
	}
	return out, nil
}

func buildTemplateRow(teamID int64, req CreateTemplateRequest) (models.TemplateRow, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return models.TemplateRow{}, ErrValidation{Msg: "name is required"}
	}
	body := strings.TrimSpace(req.Body)
	if body == "" {
		return models.TemplateRow{}, ErrValidation{Msg: "body is required"}
	}
	desc := sql.NullString{}
	if d := strings.TrimSpace(req.Description); d != "" {
		desc = sql.NullString{Valid: true, String: d}
	}
	return models.TemplateRow{
		TeamID:      teamID,
		Name:        name,
		Description: desc,
		Body:        body,
	}, nil
}

// Integrations catalog ------------------------------------------------------

// integrationCatalog is the static catalog rendered by the Integrations tab.
var integrationCatalog = []struct {
	ID    string
	Name  string
	Desc  string
	Color string
}{
	{"slack", "Slack", "Send rich messages to channels", "#611f69"},
	{"pagerduty", "PagerDuty", "Page on-call responders", "#06ac38"},
	{"opsgenie", "Opsgenie", "Atlassian incident alerts", "#172b4d"},
	{"email", "Email", "Send via SMTP relay", "#475569"},
	{"webhook", "Webhook", "POST to any URL with custom body", "#8b5cf6"},
	{"teams", "MS Teams", "Post to a Teams channel", "#4b53bc"},
	{"twilio", "Twilio SMS", "Send SMS alerts to phone numbers", "#f22f46"},
	{"jira", "Jira", "Auto-create tickets on trigger", "#2563eb"},
}

// ListIntegrations returns the catalog with channel counts and status.
func (s *Service) ListIntegrations(ctx context.Context, teamID int64) ([]IntegrationCatalogEntry, error) {
	rows, err := s.repo.ListChannels(ctx, teamID)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.Type]++
	}
	out := make([]IntegrationCatalogEntry, 0, len(integrationCatalog))
	for _, it := range integrationCatalog {
		status := "not_connected"
		if it.ID == "slack" {
			status = "connected"
		}
		out = append(out, IntegrationCatalogEntry{
			ID: it.ID, Name: it.Name, Desc: it.Desc, Color: it.Color,
			Status: status, Count: counts[it.ID],
		})
	}
	return out, nil
}
