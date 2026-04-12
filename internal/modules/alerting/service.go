package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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

	ListIncidents(ctx context.Context, teamID int64) ([]IncidentResponse, error)
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
	if err := validateCondition(req.ConditionType, s.registry); err != nil {
		return nil, err
	}
	rule := &Rule{
		TeamID:            teamID,
		Name:              req.Name,
		Description:       req.Description,
		ConditionType:     req.ConditionType,
		TargetRef:         req.TargetRef,
		GroupBy:           req.GroupBy,
		Windows:           req.Windows,
		Operator:          firstNonEmpty(req.Operator, OpGT),
		WarnThreshold:     req.WarnThreshold,
		CriticalThreshold: req.CriticalThreshold,
		RecoveryThreshold: req.RecoveryThreshold,
		ForSecs:           req.ForSecs,
		RecoverForSecs:    req.RecoverForSecs,
		KeepAliveSecs:     req.KeepAliveSecs,
		NoDataSecs:        req.NoDataSecs,
		Severity:          firstNonEmpty(req.Severity, SeverityP3),
		NotifyTemplate:    req.NotifyTemplate,
		MaxNotifsPerHour:  req.MaxNotifsPerHour,
		SlackWebhookURL:   req.SlackWebhookURL,
		Enabled:           req.Enabled,
		RuleState:         StateOK,
		Instances:         InstancesMap{},
		CreatedBy:         userID,
		UpdatedBy:         userID,
	}
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
	applyUpdate(rule, req)
	rule.UpdatedBy = userID
	if err := s.repo.UpdateRule(ctx, rule); err != nil {
		return nil, err
	}
	if err := s.repo.WriteEvent(ctx, AlertEvent{
		TeamID:  uint32(teamID), //nolint:gosec
		AlertID: rule.ID, Kind: EventKindEdit, ActorUserID: userID,
	}); err != nil {
		slog.Debug("alerting: write audit event failed", slog.Any("error", err))
	}
	return ruleToResponse(rule), nil
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

// ------------------- incidents / audit -------------------

func (s *service) ListIncidents(ctx context.Context, teamID int64) ([]IncidentResponse, error) {
	rules, err := s.repo.ListRules(ctx, teamID)
	if err != nil {
		return nil, err
	}
	out := []IncidentResponse{}
	for _, r := range rules {
		for _, inst := range r.Instances {
			if inst.State != StateAlert && inst.State != StateWarn {
				continue
			}
			out = append(out, IncidentResponse{
				AlertID:     r.ID,
				RuleName:    r.Name,
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

func applyUpdate(rule *Rule, req UpdateRuleRequest) {
	if req.Name != nil {
		rule.Name = *req.Name
	}
	if req.Description != nil {
		rule.Description = *req.Description
	}
	if req.TargetRef != nil {
		rule.TargetRef = *req.TargetRef
	}
	if req.GroupBy != nil {
		rule.GroupBy = *req.GroupBy
	}
	if req.Windows != nil {
		rule.Windows = *req.Windows
	}
	if req.Operator != nil {
		rule.Operator = *req.Operator
	}
	if req.WarnThreshold != nil {
		rule.WarnThreshold = req.WarnThreshold
	}
	if req.CriticalThreshold != nil {
		rule.CriticalThreshold = *req.CriticalThreshold
	}
	if req.RecoveryThreshold != nil {
		rule.RecoveryThreshold = req.RecoveryThreshold
	}
	if req.ForSecs != nil {
		rule.ForSecs = *req.ForSecs
	}
	if req.RecoverForSecs != nil {
		rule.RecoverForSecs = *req.RecoverForSecs
	}
	if req.KeepAliveSecs != nil {
		rule.KeepAliveSecs = *req.KeepAliveSecs
	}
	if req.NoDataSecs != nil {
		rule.NoDataSecs = *req.NoDataSecs
	}
	if req.Severity != nil {
		rule.Severity = *req.Severity
	}
	if req.NotifyTemplate != nil {
		rule.NotifyTemplate = *req.NotifyTemplate
	}
	if req.MaxNotifsPerHour != nil {
		rule.MaxNotifsPerHour = *req.MaxNotifsPerHour
	}
	if req.SlackWebhookURL != nil {
		rule.SlackWebhookURL = *req.SlackWebhookURL
	}
	if req.Enabled != nil {
		rule.Enabled = *req.Enabled
	}
}

func ruleToResponse(rule *Rule) *RuleResponse {
	insts := make([]*Instance, 0, len(rule.Instances))
	for _, inst := range rule.Instances {
		insts = append(insts, inst)
	}
	return &RuleResponse{
		ID:                rule.ID,
		TeamID:            rule.TeamID,
		Name:              rule.Name,
		Description:       rule.Description,
		ConditionType:     rule.ConditionType,
		TargetRef:         rule.TargetRef,
		GroupBy:           rule.GroupBy,
		Windows:           rule.Windows,
		Operator:          rule.Operator,
		WarnThreshold:     rule.WarnThreshold,
		CriticalThreshold: rule.CriticalThreshold,
		RecoveryThreshold: rule.RecoveryThreshold,
		ForSecs:           rule.ForSecs,
		RecoverForSecs:    rule.RecoverForSecs,
		KeepAliveSecs:     rule.KeepAliveSecs,
		NoDataSecs:        rule.NoDataSecs,
		Severity:          rule.Severity,
		NotifyTemplate:    rule.NotifyTemplate,
		MaxNotifsPerHour:  rule.MaxNotifsPerHour,
		SlackWebhookURL:   rule.SlackWebhookURL,
		RuleState:         rule.RuleState,
		LastEvalAt:        rule.LastEvalAt,
		Instances:         insts,
		MuteUntil:         rule.MuteUntil,
		Silences:          rule.Silences,
		Enabled:           rule.Enabled,
		CreatedAt:         rule.CreatedAt,
		UpdatedAt:         rule.UpdatedAt,
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
	if len(raw) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	for _, k := range []string{"service_name", "service", "serviceName"} {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

// ============================================================
// Evaluator loop — BackgroundRunner
// ============================================================

// EvaluatorLoop is the background tick worker that re-evaluates every enabled
// rule on a fixed cadence, runs the state machine, persists transitions, and
// hands changes to the dispatcher.
type EvaluatorLoop struct {
	repo       Repository
	registry   *evaluators.Registry
	dispatcher *Dispatcher
	interval   time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewEvaluatorLoop(repo Repository, reg *evaluators.Registry, dispatcher *Dispatcher) *EvaluatorLoop {
	return &EvaluatorLoop{
		repo:       repo,
		registry:   reg,
		dispatcher: dispatcher,
		interval:   30 * time.Second,
	}
}

func (l *EvaluatorLoop) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.wg.Add(1)
	go l.run(ctx)
}

func (l *EvaluatorLoop) Stop() error {
	if l.cancel != nil {
		l.cancel()
	}
	l.wg.Wait()
	return nil
}

func (l *EvaluatorLoop) run(ctx context.Context) {
	defer l.wg.Done()
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.tick(ctx)
		}
	}
}

func (l *EvaluatorLoop) tick(ctx context.Context) {
	rules, err := l.repo.ListEnabledRules(ctx)
	if err != nil {
		slog.Error("alerting: list enabled rules failed", slog.Any("error", err))
		return
	}
	now := time.Now().UTC()
	for _, rule := range rules {
		l.evalRule(ctx, rule, now)
	}
}

func (l *EvaluatorLoop) evalRule(ctx context.Context, rule *Rule, now time.Time) {
	eval, ok := l.registry.Get(rule.ConditionType)
	if !ok {
		return
	}
	results, err := eval.Evaluate(ctx, adaptRule(rule))
	if err != nil {
		slog.Error("alerting: evaluate failed",
			slog.Int64("alert_id", rule.ID), slog.Any("error", err))
		return
	}
	if rule.Instances == nil {
		rule.Instances = InstancesMap{}
	}
	changed := false
	for _, r := range results {
		inst, ok := rule.Instances[r.InstanceKey]
		if !ok {
			inst = &Instance{
				InstanceKey: r.InstanceKey,
				GroupValues: r.GroupValues,
				State:       StateOK,
			}
			rule.Instances[r.InstanceKey] = inst
		}
		inst.Values = r.Windows
		tr := Decide(rule, inst, r.Windows, r.NoData, now)
		if tr.Changed {
			inst.State = tr.ToState
			inst.LastTransitionSeq++
			changed = true
			// Deploy correlation (best effort).
			from := now.Add(-30 * time.Minute).UnixMilli()
			refs, _ := l.repo.DeploysInRange(ctx, rule.TeamID, from, now.UnixMilli()) //nolint:errcheck // best-effort deploy correlation
			valuesJSON, _ := json.Marshal(r.Windows)                                  //nolint:errcheck // marshal of known-safe struct
			deployJSON, _ := json.Marshal(refs)                                        //nolint:errcheck // marshal of known-safe struct slice
			if err := l.repo.WriteEvent(ctx, AlertEvent{
				Ts: now, TeamID: uint32(rule.TeamID), //nolint:gosec
				AlertID:      rule.ID,
				InstanceKey:  r.InstanceKey,
				Kind:         EventKindTransition,
				FromState:    tr.FromState,
				ToState:      tr.ToState,
				Values:       string(valuesJSON),
				DeployRefs:   string(deployJSON),
				TransitionID: inst.LastTransitionSeq,
			}); err != nil {
				slog.Debug("alerting: write transition event failed", slog.Any("error", err))
			}
			if l.dispatcher != nil {
				l.dispatcher.Enqueue(dispatchItem{
					Rule:       rule,
					Instance:   inst,
					Transition: tr,
					Now:        now,
					DeployRefs: refs,
				})
			}
		}
	}
	rule.RuleState = RollUpRuleState(rule.Instances)
	t := now
	rule.LastEvalAt = &t
	if changed {
		if err := l.repo.SaveRuleRuntime(ctx, rule); err != nil {
			slog.Error("alerting: save runtime failed",
				slog.Int64("alert_id", rule.ID), slog.Any("error", err))
		}
	}
}
