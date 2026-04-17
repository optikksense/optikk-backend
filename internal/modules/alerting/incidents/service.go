package incidents

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/engine"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/rules"
)

// Service covers incident listing + per-instance state changes (ack, snooze,
// activity feed). Backed by the rules.Repository (rule + inline instance
// state lives on the rule row) and the engine.EventStore (activity feed +
// audit writes).
type Service interface {
	List(ctx context.Context, teamID int64, state string) ([]IncidentResponse, error)
	ListActivity(ctx context.Context, teamID int64, limit int) ([]ActivityEntry, error)
	Ack(ctx context.Context, teamID, userID, id int64, req AckInstanceRequest) error
	Snooze(ctx context.Context, teamID, userID, id int64, req SnoozeInstanceRequest) error
}

type service struct {
	repo   rules.Repository
	events engine.EventStore
}

func NewService(repo rules.Repository, events engine.EventStore) Service {
	return &service{repo: repo, events: events}
}

func (s *service) List(ctx context.Context, teamID int64, state string) ([]IncidentResponse, error) {
	rs, err := s.repo.List(ctx, teamID)
	if err != nil {
		return nil, err
	}
	wantResolved := strings.EqualFold(strings.TrimSpace(state), "resolved")
	out := []IncidentResponse{}
	for _, r := range rs {
		def := shared.RuleDefinitionFromRow(r)
		for _, inst := range r.Instances {
			if wantResolved {
				if inst.State != shared.StateOK || inst.ResolvedAt == nil {
					continue
				}
			} else if inst.State != shared.StateAlert && inst.State != shared.StateWarn {
				continue
			}
			out = append(out, IncidentResponse{
				AlertID:     shared.Itoa(r.ID),
				RuleName:    r.Name,
				PresetKind:  def.PresetKind,
				Summary:     shared.AlertSummary(def),
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
	if s.events == nil {
		return nil, fmt.Errorf("alerting: event store unavailable")
	}
	events, err := s.events.ListRecentEvents(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	rs, err := s.repo.List(ctx, teamID)
	if err != nil {
		return nil, err
	}
	ruleIndex := make(map[int64]*shared.Rule, len(rs))
	for _, r := range rs {
		ruleIndex[r.ID] = r
	}
	out := make([]ActivityEntry, 0, len(events))
	for _, ev := range events {
		rule := ruleIndex[ev.AlertID]
		def := shared.AlertRuleDefinition{
			Name:       "Alert",
			PresetKind: shared.PresetServiceErrorRate,
			Condition:  shared.AlertRuleCondition{Threshold: 5, WindowMinutes: 5, HoldMinutes: 2, Severity: shared.SeverityP2},
			Enabled:    true,
		}
		ruleName := "Deleted rule"
		if rule != nil {
			def = shared.RuleDefinitionFromRow(rule)
			ruleName = rule.Name
		}
		entry := ActivityEntry{
			Ts:          ev.Ts,
			AlertID:     shared.Itoa(ev.AlertID),
			RuleName:    ruleName,
			PresetKind:  def.PresetKind,
			Summary:     shared.AlertSummary(def),
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

func (s *service) Ack(ctx context.Context, teamID, userID, id int64, req AckInstanceRequest) error {
	rule, err := s.repo.Get(ctx, teamID, id)
	if err != nil {
		return err
	}
	if rule == nil {
		return shared.ErrRuleNotFound
	}
	inst, ok := rule.Instances[req.InstanceKey]
	if !ok {
		return fmt.Errorf("alerting: instance %q not found", req.InstanceKey)
	}
	inst.AckedBy = userID
	if req.Until != nil {
		inst.AckedUntil = req.Until
	}
	if err := s.repo.SaveRuntime(ctx, rule); err != nil {
		return err
	}
	if s.events != nil {
		if err := s.events.WriteEvent(ctx, shared.AlertEvent{
			TeamID:  uint32(teamID), //nolint:gosec
			AlertID: rule.ID, InstanceKey: req.InstanceKey, Kind: shared.EventKindAck,
			ActorUserID: userID, Message: req.Comment,
		}); err != nil {
			slog.Debug("alerting: write audit event failed", slog.Any("error", err))
		}
	}
	return nil
}

func (s *service) Snooze(ctx context.Context, teamID, userID, id int64, req SnoozeInstanceRequest) error {
	rule, err := s.repo.Get(ctx, teamID, id)
	if err != nil {
		return err
	}
	if rule == nil {
		return shared.ErrRuleNotFound
	}
	inst, ok := rule.Instances[req.InstanceKey]
	if !ok {
		return fmt.Errorf("alerting: instance %q not found", req.InstanceKey)
	}
	until := time.Now().UTC().Add(time.Duration(req.Minutes) * time.Minute)
	inst.SnoozedUntil = &until
	if err := s.repo.SaveRuntime(ctx, rule); err != nil {
		return err
	}
	if s.events != nil {
		if err := s.events.WriteEvent(ctx, shared.AlertEvent{
			TeamID:  uint32(teamID), //nolint:gosec
			AlertID: rule.ID, InstanceKey: req.InstanceKey, Kind: shared.EventKindAck, ActorUserID: userID,
			Message: fmt.Sprintf("snoozed %d minutes", req.Minutes),
		}); err != nil {
			slog.Debug("alerting: write audit event failed", slog.Any("error", err))
		}
	}
	return nil
}
