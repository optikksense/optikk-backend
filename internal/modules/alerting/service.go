package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
)

// ErrUnsupportedCondition is returned when a rule specifies a condition_type
// that has no registered evaluator.
var ErrUnsupportedCondition = errors.New("alerting: unsupported condition type")

// ErrRuleNotFound is returned by GetRule / UpdateRule / DeleteRule when the
// requested rule does not exist in the caller's team.
var ErrRuleNotFound = errors.New("alerting: rule not found")

// Service is the public contract used by handler.go. It owns both the HTTP
// code path and the evaluator background loop (which runs as a separate
// BackgroundRunner wired from module.go).
type Service interface {
	CreateRule(ctx context.Context, teamID, userID int64, req CreateRuleRequest) (*RuleResponse, error)
	UpdateRule(ctx context.Context, teamID, userID, id int64, req UpdateRuleRequest) (*RuleResponse, error)
	DeleteRule(ctx context.Context, teamID, id int64) error
	GetRule(ctx context.Context, teamID, id int64) (*RuleResponse, error)
	ListRules(ctx context.Context, teamID int64) ([]*RuleResponse, error)
	PreviewRule(ctx context.Context, req CreateRuleRequest) (*PreviewRuleResponse, error)
	TestSlack(ctx context.Context, req SlackTestRequest) (*SlackTestResponse, error)

	ListIncidents(ctx context.Context, teamID int64, state string) ([]IncidentResponse, error)
	ListActivity(ctx context.Context, teamID int64, limit int) ([]ActivityEntry, error)
	MuteRule(ctx context.Context, teamID, userID, id int64, req MuteRuleRequest) error
	TestRule(ctx context.Context, teamID, id int64) (*TestRuleResponse, error)
	BacktestRule(ctx context.Context, teamID, id int64, req BacktestRequest) (*BacktestResponse, error)

	AckInstance(ctx context.Context, teamID, userID, id int64, req AckInstanceRequest) error
	SnoozeInstance(ctx context.Context, teamID, userID, id int64, req SnoozeInstanceRequest) error

	ListSilences(ctx context.Context, teamID int64) ([]Silence, error)
	CreateSilence(ctx context.Context, teamID, userID int64, req CreateSilenceRequest) (*Silence, error)
	UpdateSilence(ctx context.Context, teamID int64, id int64, silenceID string, req UpdateSilenceRequest) (*Silence, error)
	DeleteSilence(ctx context.Context, teamID, id int64, silenceID string) error

	ListAudit(ctx context.Context, teamID, id int64, limit int) ([]AuditEntry, error)
	HandleSlackCallback(ctx context.Context, payload map[string]any) error
}

type service struct {
	repo       Repository
	registry   *evaluators.Registry
	dispatcher *Dispatcher
}

// NewService wires the HTTP-side service.
func NewService(repo Repository, reg *evaluators.Registry, dispatcher *Dispatcher) Service {
	return &service{repo: repo, registry: reg, dispatcher: dispatcher}
}

// ------------------- rules CRUD -------------------

func (s *service) CreateRule(ctx context.Context, teamID, userID int64, req CreateRuleRequest) (*RuleResponse, error) {
	def := normalizeAlertRuleDefinition(AlertRuleDefinition{
		Name:        req.Name,
		Description: req.Description,
		PresetKind:  req.PresetKind,
		Scope:       req.Scope,
		Condition:   req.Condition,
		Delivery:    req.Delivery,
		Enabled:     req.Enabled,
	})
	rule, err := engineRuleFromDefinition(def, nil)
	if err != nil {
		return nil, err
	}
	if err := validateCondition(rule.ConditionType, s.registry); err != nil {
		return nil, err
	}
	rule.TeamID = teamID
	rule.RuleState = StateOK
	rule.Instances = InstancesMap{}
	rule.CreatedBy = userID
	rule.UpdatedBy = userID
	if _, err := s.repo.CreateRule(ctx, rule); err != nil {
		return nil, err
	}
	return ruleToResponse(rule), nil
}

func (s *service) UpdateRule(ctx context.Context, teamID, userID, id int64, req UpdateRuleRequest) (*RuleResponse, error) {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ErrRuleNotFound
	}
	merged := applyUpdate(ruleDefinitionFromRow(rule), req)
	nextRule, err := engineRuleFromDefinition(merged, rule)
	if err != nil {
		return nil, err
	}
	if err := validateCondition(nextRule.ConditionType, s.registry); err != nil {
		return nil, err
	}
	nextRule.TeamID = teamID
	nextRule.UpdatedBy = userID
	if err := s.repo.UpdateRule(ctx, nextRule); err != nil {
		return nil, err
	}
	if err := s.repo.WriteEvent(ctx, AlertEvent{
		TeamID:  uint32(teamID), //nolint:gosec
		AlertID: nextRule.ID, Kind: EventKindEdit, ActorUserID: userID,
	}); err != nil {
		slog.Debug("alerting: write audit event failed", slog.Any("error", err))
	}
	return ruleToResponse(nextRule), nil
}

func (s *service) DeleteRule(ctx context.Context, teamID, id int64) error {
	return s.repo.DeleteRule(ctx, teamID, id)
}

func (s *service) GetRule(ctx context.Context, teamID, id int64) (*RuleResponse, error) {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ErrRuleNotFound
	}
	return ruleToResponse(rule), nil
}

func (s *service) ListRules(ctx context.Context, teamID int64) ([]*RuleResponse, error) {
	rules, err := s.repo.ListRules(ctx, teamID)
	if err != nil {
		return nil, err
	}
	out := make([]*RuleResponse, 0, len(rules))
	for _, r := range rules {
		out = append(out, ruleToResponse(r))
	}
	return out, nil
}

func (s *service) PreviewRule(_ context.Context, req CreateRuleRequest) (*PreviewRuleResponse, error) {
	def := normalizeAlertRuleDefinition(AlertRuleDefinition{
		Name:        req.Name,
		Description: req.Description,
		PresetKind:  req.PresetKind,
		Scope:       req.Scope,
		Condition:   req.Condition,
		Delivery:    req.Delivery,
		Enabled:     req.Enabled,
	})
	rule, err := engineRuleFromDefinition(def, nil)
	if err != nil {
		return nil, err
	}
	preview := previewNotification(def)
	return &PreviewRuleResponse{
		Summary: alertSummary(def),
		Engine: RuleEnginePreview{
			ConditionType:     rule.ConditionType,
			Operator:          rule.Operator,
			Windows:           rule.Windows,
			CriticalThreshold: rule.CriticalThreshold,
			RecoveryThreshold: rule.RecoveryThreshold,
			ForSecs:           rule.ForSecs,
			RecoverForSecs:    rule.RecoverForSecs,
			NoDataSecs:        rule.NoDataSecs,
			Severity:          rule.Severity,
		},
		Notification: preview,
	}, nil
}

func (s *service) TestSlack(ctx context.Context, req SlackTestRequest) (*SlackTestResponse, error) {
	def := normalizeAlertRuleDefinition(req.Rule)
	if err := validateAlertRuleDefinition(def); err != nil {
		return nil, err
	}
	preview := previewNotification(def)
	rendered := renderSlackMessage(def, preview, "")
	if s.dispatcher == nil {
		return &SlackTestResponse{Delivered: false, Error: "slack dispatcher unavailable", Notification: preview}, nil
	}
	if err := s.dispatcher.SendSlack(ctx, def.Delivery.SlackWebhookURL, rendered); err != nil {
		return &SlackTestResponse{Delivered: false, Error: err.Error(), Notification: preview}, nil
	}
	return &SlackTestResponse{Delivered: true, Notification: preview}, nil
}

// ------------------- incidents / audit -------------------

func (s *service) ListIncidents(ctx context.Context, teamID int64, state string) ([]IncidentResponse, error) {
	rules, err := s.repo.ListRules(ctx, teamID)
	if err != nil {
		return nil, err
	}
	wantResolved := strings.EqualFold(strings.TrimSpace(state), "resolved")
	out := []IncidentResponse{}
	for _, r := range rules {
		def := ruleDefinitionFromRow(r)
		for _, inst := range r.Instances {
			if wantResolved {
				if inst.State != StateOK || inst.ResolvedAt == nil {
					continue
				}
			} else if inst.State != StateAlert && inst.State != StateWarn {
				continue
			}
			out = append(out, IncidentResponse{
				AlertID:     itoa(r.ID),
				RuleName:    r.Name,
				PresetKind:  def.PresetKind,
				Summary:     alertSummary(def),
				Severity:    r.Severity,
				InstanceKey: inst.InstanceKey,
				GroupValues: inst.GroupValues,
				State:       inst.State,
				FiredAt:     inst.FiredAt,
				Values:      inst.Values,
			})
		}
	}
	return out, nil
}

func (s *service) ListActivity(ctx context.Context, teamID int64, limit int) ([]ActivityEntry, error) {
	events, err := s.repo.ListRecentEvents(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	rules, err := s.repo.ListRules(ctx, teamID)
	if err != nil {
		return nil, err
	}
	ruleIndex := make(map[int64]*Rule, len(rules))
	for _, rule := range rules {
		ruleIndex[rule.ID] = rule
	}
	out := make([]ActivityEntry, 0, len(events))
	for _, ev := range events {
		rule := ruleIndex[ev.AlertID]
		def := AlertRuleDefinition{
			Name:       "Alert",
			PresetKind: PresetServiceErrorRate,
			Condition:  AlertRuleCondition{Threshold: 5, WindowMinutes: 5, HoldMinutes: 2, Severity: SeverityP2},
			Enabled:    true,
		}
		ruleName := "Deleted rule"
		if rule != nil {
			def = ruleDefinitionFromRow(rule)
			ruleName = rule.Name
		}
		entry := ActivityEntry{
			Ts:          ev.Ts,
			AlertID:     itoa(ev.AlertID),
			RuleName:    ruleName,
			PresetKind:  def.PresetKind,
			Summary:     alertSummary(def),
			Kind:        ev.Kind,
			FromState:   ev.FromState,
			ToState:     ev.ToState,
			InstanceKey: ev.InstanceKey,
			ActorUserID: ev.ActorUserID,
			Message:     ev.Message,
		}
		if strings.TrimSpace(ev.Values) != "" {
			var values map[string]float64
			if err := json.Unmarshal([]byte(ev.Values), &values); err == nil {
				entry.Values = values
			}
		}
		out = append(out, entry)
	}
	return out, nil
}

func (s *service) ListAudit(ctx context.Context, teamID, id int64, limit int) ([]AuditEntry, error) {
	events, err := s.repo.ListEvents(ctx, teamID, id, limit)
	if err != nil {
		return nil, err
	}
	out := make([]AuditEntry, 0, len(events))
	for _, ev := range events {
		out = append(out, AuditEntry{
			Ts:          ev.Ts,
			Kind:        ev.Kind,
			FromState:   ev.FromState,
			ToState:     ev.ToState,
			InstanceKey: ev.InstanceKey,
			ActorUserID: ev.ActorUserID,
			Message:     ev.Message,
		})
	}
	return out, nil
}

// ------------------- mute / test / backtest -------------------

func (s *service) MuteRule(ctx context.Context, teamID, userID, id int64, req MuteRuleRequest) error {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return err
	}
	if rule == nil {
		return ErrRuleNotFound
	}
	t := req.Until
	rule.MuteUntil = &t
	rule.UpdatedBy = userID
	if err := s.repo.UpdateRule(ctx, rule); err != nil {
		return err
	}
	if err := s.repo.WriteEvent(ctx, AlertEvent{
		TeamID:  uint32(teamID), //nolint:gosec
		AlertID: rule.ID, Kind: EventKindMute, ActorUserID: userID, Message: req.Reason,
	}); err != nil {
		slog.Debug("alerting: write audit event failed", slog.Any("error", err))
	}
	return nil
}

func (s *service) TestRule(ctx context.Context, teamID, id int64) (*TestRuleResponse, error) {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ErrRuleNotFound
	}
	eval, ok := s.registry.Get(rule.ConditionType)
	if !ok {
		return nil, ErrUnsupportedCondition
	}
	results, err := eval.Evaluate(ctx, adaptRule(rule))
	if err != nil {
		return nil, err
	}
	resp := &TestRuleResponse{EvaluatedAt: time.Now().UTC()}
	for _, r := range results {
		would := !r.NoData && crossesAny(r.Windows, rule.Operator, rule.CriticalThreshold)
		if would {
			resp.WouldFire = true
		}
		resp.Results = append(resp.Results, TestInstanceResult{
			InstanceKey: r.InstanceKey,
			GroupValues: r.GroupValues,
			Values:      r.Windows,
			NoData:      r.NoData,
			WouldFire:   would,
		})
	}
	return resp, nil
}

func (s *service) BacktestRule(ctx context.Context, teamID, id int64, req BacktestRequest) (*BacktestResponse, error) {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ErrRuleNotFound
	}
	resp, err := RunBacktest(ctx, s.registry, rule, req.FromMs, req.ToMs, req.StepMs)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// ------------------- ack / snooze -------------------

func (s *service) AckInstance(ctx context.Context, teamID, userID, id int64, req AckInstanceRequest) error {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return err
	}
	if rule == nil {
		return ErrRuleNotFound
	}
	inst, ok := rule.Instances[req.InstanceKey]
	if !ok {
		return fmt.Errorf("alerting: instance %q not found", req.InstanceKey)
	}
	inst.AckedBy = userID
	if req.Until != nil {
		inst.AckedUntil = req.Until
	}
	if err := s.repo.SaveRuleRuntime(ctx, rule); err != nil {
		return err
	}
	if err := s.repo.WriteEvent(ctx, AlertEvent{
		TeamID:  uint32(teamID), //nolint:gosec
		AlertID: rule.ID, InstanceKey: req.InstanceKey, Kind: EventKindAck,
		ActorUserID: userID, Message: req.Comment,
	}); err != nil {
		slog.Debug("alerting: write audit event failed", slog.Any("error", err))
	}
	return nil
}

func (s *service) SnoozeInstance(ctx context.Context, teamID, userID, id int64, req SnoozeInstanceRequest) error {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return err
	}
	if rule == nil {
		return ErrRuleNotFound
	}
	inst, ok := rule.Instances[req.InstanceKey]
	if !ok {
		return fmt.Errorf("alerting: instance %q not found", req.InstanceKey)
	}
	until := time.Now().UTC().Add(time.Duration(req.Minutes) * time.Minute)
	inst.SnoozedUntil = &until
	if err := s.repo.SaveRuleRuntime(ctx, rule); err != nil {
		return err
	}
	if err := s.repo.WriteEvent(ctx, AlertEvent{
		TeamID:  uint32(teamID), //nolint:gosec
		AlertID: rule.ID, InstanceKey: req.InstanceKey, Kind: EventKindAck, ActorUserID: userID,
		Message: fmt.Sprintf("snoozed %d minutes", req.Minutes),
	}); err != nil {
		slog.Debug("alerting: write audit event failed", slog.Any("error", err))
	}
	return nil
}

// ------------------- silences -------------------

func (s *service) ListSilences(ctx context.Context, teamID int64) ([]Silence, error) {
	rules, err := s.repo.ListRules(ctx, teamID)
	if err != nil {
		return nil, err
	}
	var out []Silence
	for _, r := range rules {
		out = append(out, r.Silences...)
	}
	return out, nil
}

func (s *service) CreateSilence(ctx context.Context, teamID, userID int64, req CreateSilenceRequest) (*Silence, error) {
	if req.AlertID == 0 {
		return nil, errors.New("alerting: silence requires alert_id")
	}
	rule, err := s.repo.GetRule(ctx, teamID, req.AlertID)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ErrRuleNotFound
	}
	sil := Silence{
		ID:         fmt.Sprintf("sil_%d", time.Now().UnixNano()),
		StartsAt:   req.StartsAt,
		EndsAt:     req.EndsAt,
		Recurrence: req.Recurrence,
		MatchTags:  req.MatchTags,
		Reason:     req.Reason,
		CreatedBy:  userID,
	}
	rule.Silences = append(rule.Silences, sil)
	if err := s.repo.UpdateRule(ctx, rule); err != nil {
		return nil, err
	}
	if err := s.repo.WriteEvent(ctx, AlertEvent{
		TeamID:  uint32(teamID), //nolint:gosec
		AlertID: rule.ID, Kind: EventKindSilence, ActorUserID: userID,
	}); err != nil {
		slog.Debug("alerting: write audit event failed", slog.Any("error", err))
	}
	return &sil, nil
}

func (s *service) UpdateSilence(ctx context.Context, teamID int64, id int64, silenceID string, req UpdateSilenceRequest) (*Silence, error) {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, ErrRuleNotFound
	}
	var updated *Silence
	for i := range rule.Silences {
		if rule.Silences[i].ID == silenceID {
			if req.StartsAt != nil {
				rule.Silences[i].StartsAt = *req.StartsAt
			}
			if req.EndsAt != nil {
				rule.Silences[i].EndsAt = *req.EndsAt
			}
			if req.Recurrence != nil {
				rule.Silences[i].Recurrence = *req.Recurrence
			}
			if req.MatchTags != nil {
				rule.Silences[i].MatchTags = *req.MatchTags
			}
			if req.Reason != nil {
				rule.Silences[i].Reason = *req.Reason
			}
			sil := rule.Silences[i]
			updated = &sil
			break
		}
	}
	if updated == nil {
		return nil, errors.New("alerting: silence not found")
	}
	if err := s.repo.UpdateRule(ctx, rule); err != nil {
		return nil, err
	}
	return updated, nil
}

func (s *service) DeleteSilence(ctx context.Context, teamID, id int64, silenceID string) error {
	rule, err := s.repo.GetRule(ctx, teamID, id)
	if err != nil {
		return err
	}
	if rule == nil {
		return ErrRuleNotFound
	}
	out := rule.Silences[:0]
	for _, sil := range rule.Silences {
		if sil.ID != silenceID {
			out = append(out, sil)
		}
	}
	rule.Silences = out
	return s.repo.UpdateRule(ctx, rule)
}

// ------------------- slack callback -------------------

func (s *service) HandleSlackCallback(ctx context.Context, payload map[string]any) error {
	// v1: log + accept. A signed callback implementation requires sharing a
	// secret and parsing the Slack interactivity envelope; kept as a stub so
	// the route is wired but non-blocking.
	slog.Info("alerting: slack callback received", slog.Any("payload", payload))
	return nil
}

// ------------------- helpers -------------------

func validateCondition(kind string, reg *evaluators.Registry) error {
	if _, ok := reg.Get(kind); !ok {
		return ErrUnsupportedCondition
	}
	return nil
}

func applyUpdate(def AlertRuleDefinition, req UpdateRuleRequest) AlertRuleDefinition {
	if req.Name != nil {
		def.Name = *req.Name
	}
	if req.Description != nil {
		def.Description = *req.Description
	}
	if req.PresetKind != nil {
		def.PresetKind = *req.PresetKind
	}
	if req.Scope != nil {
		def.Scope = *req.Scope
	}
	if req.Condition != nil {
		def.Condition = *req.Condition
	}
	if req.Delivery != nil {
		def.Delivery = *req.Delivery
	}
	if req.Enabled != nil {
		def.Enabled = *req.Enabled
	}
	return normalizeAlertRuleDefinition(def)
}

func ruleToResponse(rule *Rule) *RuleResponse {
	def := ruleDefinitionFromRow(rule)
	insts := make([]*Instance, 0, len(rule.Instances))
	for _, inst := range rule.Instances {
		insts = append(insts, inst)
	}
	return &RuleResponse{
		ID:          itoa(rule.ID),
		TeamID:      rule.TeamID,
		Name:        def.Name,
		Description: def.Description,
		PresetKind:  def.PresetKind,
		Scope:       def.Scope,
		Condition:   def.Condition,
		Delivery:    def.Delivery,
		Summary:     alertSummary(def),
		RuleState:   rule.RuleState,
		LastEvalAt:  rule.LastEvalAt,
		Instances:   insts,
		MuteUntil:   rule.MuteUntil,
		Silences:    rule.Silences,
		Enabled:     rule.Enabled,
		CreatedAt:   rule.CreatedAt,
		UpdatedAt:   rule.UpdatedAt,
	}
}

func firstNonEmpty(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

// targetServiceFromRef extracts {"service_name":"..."} from a rule's TargetRef.
// Used by backtest.go and the evaluator loop.
func targetServiceFromRef(raw json.RawMessage) string {
	m := targetRefMap(raw)
	for _, k := range []string{"service_name", "service", "serviceName"} {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

