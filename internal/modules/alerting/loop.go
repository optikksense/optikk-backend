package alerting

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
)

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
			l.recordTransition(ctx, rule, r, inst, tr, now)
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

func (l *EvaluatorLoop) recordTransition(ctx context.Context, rule *Rule, r evaluators.InstanceResult, inst *Instance, tr Transition, now time.Time) {
	from := now.Add(-30 * time.Minute).UnixMilli()
	refs, _ := l.repo.DeploysInRange(ctx, rule.TeamID, from, now.UnixMilli()) //nolint:errcheck // best-effort deploy correlation
	valuesJSON, _ := json.Marshal(r.Windows)                                  //nolint:errcheck // marshal of known-safe struct
	deployJSON, _ := json.Marshal(refs)                                       //nolint:errcheck // marshal of known-safe struct slice
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
