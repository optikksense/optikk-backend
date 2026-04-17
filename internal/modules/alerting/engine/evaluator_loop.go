package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
)

// RuleStore is the narrow rule I/O contract the evaluator loop depends on.
// Satisfied by the concrete rules.Repository; declared here so the import
// graph stays one-directional (rules → alerting types, engine → rules via
// this interface or not at all).
type RuleStore interface {
	ListEnabledRules(ctx context.Context) ([]*shared.Rule, error)
	SaveRuleRuntime(ctx context.Context, r *shared.Rule) error
}

// EvaluatorLoop is the background tick worker. Ticks every 30s (configurable),
// acquires a Redis lease per rule so exactly one pod evaluates each rule,
// runs the registered Evaluator, applies the state machine, writes audit
// events, enqueues notifications (durable via outbox + fast-path via dispatcher),
// and persists runtime state on transition.
type EvaluatorLoop struct {
	store      RuleStore
	events     EventStore
	deploys    deploySource
	registry   *evaluators.Registry
	dispatcher *Dispatcher
	leaser     *Leaser
	outbox     *OutboxStore
	interval   time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type deploySource interface {
	DeploysInRange(ctx context.Context, teamID int64, fromMs, toMs int64) ([]shared.DeployRef, error)
}

// NewEvaluatorLoop wires the background tick worker.
func NewEvaluatorLoop(
	store RuleStore,
	events EventStore,
	deploys DataSource,
	reg *evaluators.Registry,
	dispatcher *Dispatcher,
	lease *Leaser,
	outbox *OutboxStore,
) *EvaluatorLoop {
	return &EvaluatorLoop{
		store:      store,
		events:     events,
		deploys:    deploys,
		registry:   reg,
		dispatcher: dispatcher,
		leaser:     lease,
		outbox:     outbox,
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
	rules, err := l.store.ListEnabledRules(ctx)
	if err != nil {
		slog.Error("alerting: list enabled rules failed", slog.Any("error", err))
		return
	}
	now := time.Now().UTC()
	for _, rule := range rules {
		l.evalRule(ctx, rule, now)
	}
}

func (l *EvaluatorLoop) evalRule(ctx context.Context, rule *shared.Rule, now time.Time) {
	// Redis lease: exactly one pod evaluates a given rule per tick. Fail-closed
	// on Redis errors to prevent double-dispatch.
	if !l.leaser.Acquire(ctx, rule.ID) {
		return
	}
	eval, ok := l.registry.Get(rule.ConditionType)
	if !ok {
		return
	}
	results, err := eval.Evaluate(ctx, shared.AdaptRule(rule))
	if err != nil {
		slog.Error("alerting: evaluate failed",
			slog.Int64("alert_id", rule.ID), slog.Any("error", err))
		return
	}
	if rule.Instances == nil {
		rule.Instances = shared.InstancesMap{}
	}
	changed := false
	for _, r := range results {
		inst, ok := rule.Instances[r.InstanceKey]
		if !ok {
			inst = &shared.Instance{
				InstanceKey: r.InstanceKey,
				GroupValues: r.GroupValues,
				State:       shared.StateOK,
			}
			rule.Instances[r.InstanceKey] = inst
		}
		inst.Values = r.Windows
		tr := shared.Decide(rule, inst, r.Windows, r.NoData, now)
		if tr.Changed {
			inst.State = tr.ToState
			inst.LastTransitionSeq++
			changed = true
			from := now.Add(-30 * time.Minute).UnixMilli()
			refs, _ := l.deploys.DeploysInRange(ctx, rule.TeamID, from, now.UnixMilli()) //nolint:errcheck // best-effort deploy correlation
			valuesJSON, _ := json.Marshal(r.Windows)                                     //nolint:errcheck // marshal of known-safe struct
			deployJSON, _ := json.Marshal(refs)                                          //nolint:errcheck // marshal of known-safe struct slice
			if l.events != nil {
				if err := l.events.WriteEvent(ctx, shared.AlertEvent{
					Ts: now, TeamID: uint32(rule.TeamID), //nolint:gosec
					AlertID:      rule.ID,
					InstanceKey:  r.InstanceKey,
					Kind:         shared.EventKindTransition,
					FromState:    tr.FromState,
					ToState:      tr.ToState,
					Values:       string(valuesJSON),
					DeployRefs:   string(deployJSON),
					TransitionID: inst.LastTransitionSeq,
				}); err != nil {
					slog.Debug("alerting: write transition event failed", slog.Any("error", err))
				}
			}
			// Durable outbox write: every transition persisted so a crash
			// between here and the fast-path Slack send does not drop the
			// notification.
			if l.outbox != nil {
				deployHint := ""
				if len(refs) > 0 {
					ref := refs[len(refs)-1]
					deployHint = ref.ServiceName + " " + ref.Version
				}
				vals := make(map[string]any, len(r.Windows))
				for k, v := range r.Windows {
					vals[k] = v
				}
				if err := l.outbox.Enqueue(ctx, rule.TeamID, rule.ID, r.InstanceKey, inst.LastTransitionSeq, OutboxPayload{
					RuleID:          rule.ID,
					TeamID:          rule.TeamID,
					RuleName:        rule.Name,
					SlackWebhookURL: rule.SlackWebhookURL,
					ToState:         tr.ToState,
					Values:          vals,
					DeployHint:      deployHint,
					Tags:            inst.GroupValues,
				}); err != nil {
					slog.Debug("alerting: outbox enqueue failed", slog.Any("error", err))
				}
			}
			if l.dispatcher != nil {
				l.dispatcher.Enqueue(rule, inst, tr, now, refs)
			}
		}
	}
	rule.RuleState = shared.RollUpRuleState(rule.Instances)
	t := now
	rule.LastEvalAt = &t
	if changed {
		if err := l.store.SaveRuleRuntime(ctx, rule); err != nil {
			slog.Error("alerting: save runtime failed",
				slog.Int64("alert_id", rule.ID), slog.Any("error", err))
		}
	}
}
