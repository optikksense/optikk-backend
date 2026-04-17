package rules

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/engine"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
)

// Service is the rule-centric API surface — CRUD + preview + mute + dry-run +
// backtest + audit. Instance ack/snooze + silence CRUD live in their own
// submodules even though they mutate the same underlying rule row.
type Service interface {
	Create(ctx context.Context, teamID, userID int64, req CreateRuleRequest) (*RuleResponse, error)
	Update(ctx context.Context, teamID, userID, id int64, req UpdateRuleRequest) (*RuleResponse, error)
	Delete(ctx context.Context, teamID, id int64) error
	Get(ctx context.Context, teamID, id int64) (*RuleResponse, error)
	List(ctx context.Context, teamID int64) ([]*RuleResponse, error)
	Preview(ctx context.Context, req CreateRuleRequest) (*PreviewRuleResponse, error)
	Mute(ctx context.Context, teamID, userID, id int64, req MuteRuleRequest) error
	Test(ctx context.Context, teamID, id int64) (*TestRuleResponse, error)
	Backtest(ctx context.Context, teamID, id int64, req BacktestRequest) (*BacktestResponse, error)
	ListAudit(ctx context.Context, teamID, id int64, limit int) ([]AuditEntry, error)
}

type service struct {
	repo       Repository
	events     engine.EventStore
	registry   *evaluators.Registry
	backtester *engine.Backtester
}

func NewService(repo Repository, events engine.EventStore, registry *evaluators.Registry, backtester *engine.Backtester) Service {
	return &service{repo: repo, events: events, registry: registry, backtester: backtester}
}

func (s *service) Create(ctx context.Context, teamID, userID int64, req CreateRuleRequest) (*RuleResponse, error) {
	def := shared.NormalizeAlertRuleDefinition(shared.AlertRuleDefinition{
		Name: req.Name, Description: req.Description, PresetKind: req.PresetKind,
		Scope: req.Scope, Condition: req.Condition, Delivery: req.Delivery, Enabled: req.Enabled,
	})
	rule, err := shared.EngineRuleFromDefinition(def, nil)
	if err != nil {
		return nil, err
	}
	if _, ok := s.registry.Get(rule.ConditionType); !ok {
		return nil, shared.ErrUnsupportedCondition
	}
	rule.TeamID = teamID
	rule.RuleState = shared.StateOK
	rule.Instances = shared.InstancesMap{}
	rule.CreatedBy = userID
	rule.UpdatedBy = userID
	if _, err := s.repo.Create(ctx, rule); err != nil {
		return nil, err
	}
	return ruleToResponse(rule), nil
}

func (s *service) Update(ctx context.Context, teamID, userID, id int64, req UpdateRuleRequest) (*RuleResponse, error) {
	rule, err := s.repo.Get(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, shared.ErrRuleNotFound
	}
	merged := applyUpdate(shared.RuleDefinitionFromRow(rule), req)
	next, err := shared.EngineRuleFromDefinition(merged, rule)
	if err != nil {
		return nil, err
	}
	if _, ok := s.registry.Get(next.ConditionType); !ok {
		return nil, shared.ErrUnsupportedCondition
	}
	next.TeamID = teamID
	next.UpdatedBy = userID
	if err := s.repo.Update(ctx, next); err != nil {
		return nil, err
	}
	if s.events != nil {
		if err := s.events.WriteEvent(ctx, shared.AlertEvent{
			TeamID:  uint32(teamID), //nolint:gosec
			AlertID: next.ID, Kind: shared.EventKindEdit, ActorUserID: userID,
		}); err != nil {
			slog.Debug("alerting: write audit event failed", slog.Any("error", err))
		}
	}
	return ruleToResponse(next), nil
}

func (s *service) Delete(ctx context.Context, teamID, id int64) error {
	return s.repo.Delete(ctx, teamID, id)
}

func (s *service) Get(ctx context.Context, teamID, id int64) (*RuleResponse, error) {
	rule, err := s.repo.Get(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, shared.ErrRuleNotFound
	}
	return ruleToResponse(rule), nil
}

func (s *service) List(ctx context.Context, teamID int64) ([]*RuleResponse, error) {
	rules, err := s.repo.List(ctx, teamID)
	if err != nil {
		return nil, err
	}
	out := make([]*RuleResponse, 0, len(rules))
	for _, r := range rules {
		out = append(out, ruleToResponse(r))
	}
	return out, nil
}

func (s *service) Preview(_ context.Context, req CreateRuleRequest) (*PreviewRuleResponse, error) {
	def := shared.NormalizeAlertRuleDefinition(shared.AlertRuleDefinition{
		Name: req.Name, Description: req.Description, PresetKind: req.PresetKind,
		Scope: req.Scope, Condition: req.Condition, Delivery: req.Delivery, Enabled: req.Enabled,
	})
	rule, err := shared.EngineRuleFromDefinition(def, nil)
	if err != nil {
		return nil, err
	}
	return &PreviewRuleResponse{
		Summary: shared.AlertSummary(def),
		Engine: shared.RuleEnginePreview{
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
		Notification: shared.PreviewNotification(def),
	}, nil
}

func (s *service) Mute(ctx context.Context, teamID, userID, id int64, req MuteRuleRequest) error {
	rule, err := s.repo.Get(ctx, teamID, id)
	if err != nil {
		return err
	}
	if rule == nil {
		return shared.ErrRuleNotFound
	}
	t := req.Until
	rule.MuteUntil = &t
	rule.UpdatedBy = userID
	if err := s.repo.Update(ctx, rule); err != nil {
		return err
	}
	if s.events != nil {
		if err := s.events.WriteEvent(ctx, shared.AlertEvent{
			TeamID:  uint32(teamID), //nolint:gosec
			AlertID: rule.ID, Kind: shared.EventKindMute, ActorUserID: userID, Message: req.Reason,
		}); err != nil {
			slog.Debug("alerting: write audit event failed", slog.Any("error", err))
		}
	}
	return nil
}

func (s *service) Test(ctx context.Context, teamID, id int64) (*TestRuleResponse, error) {
	rule, err := s.repo.Get(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, shared.ErrRuleNotFound
	}
	eval, ok := s.registry.Get(rule.ConditionType)
	if !ok {
		return nil, shared.ErrUnsupportedCondition
	}
	results, err := eval.Evaluate(ctx, shared.AdaptRule(rule))
	if err != nil {
		return nil, err
	}
	resp := &TestRuleResponse{EvaluatedAt: time.Now().UTC()}
	for _, r := range results {
		would := !r.NoData && shared.CrossesAny(r.Windows, rule.Operator, rule.CriticalThreshold)
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

func (s *service) Backtest(ctx context.Context, teamID, id int64, req BacktestRequest) (*BacktestResponse, error) {
	rule, err := s.repo.Get(ctx, teamID, id)
	if err != nil {
		return nil, err
	}
	if rule == nil {
		return nil, shared.ErrRuleNotFound
	}
	resp, err := s.backtester.Run(ctx, rule, req.FromMs, req.ToMs, req.StepMs)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (s *service) ListAudit(ctx context.Context, teamID, id int64, limit int) ([]AuditEntry, error) {
	if s.events == nil {
		return nil, errors.New("alerting: event store unavailable")
	}
	events, err := s.events.ListEvents(ctx, teamID, id, limit)
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

// applyUpdate merges a PATCH request onto an existing definition. Kept in the
// rules subpackage because only rule CRUD uses it.
func applyUpdate(def shared.AlertRuleDefinition, req UpdateRuleRequest) shared.AlertRuleDefinition {
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
	return shared.NormalizeAlertRuleDefinition(def)
}

// ruleToResponse is the shape projected back to the wire. Lives here so the
// list/get/create/update endpoints share exactly one renderer.
func ruleToResponse(rule *shared.Rule) *RuleResponse {
	def := shared.RuleDefinitionFromRow(rule)
	insts := make([]*shared.Instance, 0, len(rule.Instances))
	for _, inst := range rule.Instances {
		insts = append(insts, inst)
	}
	return &RuleResponse{
		ID:          shared.Itoa(rule.ID),
		TeamID:      rule.TeamID,
		Name:        def.Name,
		Description: def.Description,
		PresetKind:  def.PresetKind,
		Scope:       def.Scope,
		Condition:   def.Condition,
		Delivery:    def.Delivery,
		Summary:     shared.AlertSummary(def),
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
