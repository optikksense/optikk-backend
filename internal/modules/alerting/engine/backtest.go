package engine

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
)

// Backtester replays a rule across [fromMs, toMs] at stepMs granularity using
// the historical evaluator path. No state writes, no dispatches. Used by
// rules.Service.BacktestRule.
type Backtester struct {
	registry *evaluators.Registry
}

func NewBacktester(reg *evaluators.Registry) *Backtester {
	return &Backtester{registry: reg}
}

// Run drives the evaluator backwards across a historical window and records
// every state transition the rule would have produced.
func (b *Backtester) Run(ctx context.Context, rule *shared.Rule, fromMs, toMs, stepMs int64) (shared.BacktestResponse, error) {
	if stepMs <= 0 {
		stepMs = 60_000
	}
	eval, ok := b.registry.Get(rule.ConditionType)
	if !ok {
		return shared.BacktestResponse{}, shared.ErrUnsupportedCondition
	}

	var resp shared.BacktestResponse
	state := make(shared.InstancesMap)
	ticks := 0
	for t := fromMs + stepMs; t <= toMs; t += stepMs {
		ticks++
		results, err := eval.EvaluateAt(ctx, shared.AdaptRule(rule), t-stepMs, t)
		if err != nil {
			return shared.BacktestResponse{}, err
		}
		for _, res := range results {
			inst, ok := state[res.InstanceKey]
			if !ok {
				inst = &shared.Instance{InstanceKey: res.InstanceKey, State: shared.StateOK, GroupValues: res.GroupValues}
				state[res.InstanceKey] = inst
			}
			now := time.UnixMilli(t)
			tr := shared.Decide(rule, inst, res.Windows, res.NoData, now)
			inst.Values = res.Windows
			if tr.Changed {
				inst.State = tr.ToState
				resp.Transitions = append(resp.Transitions, shared.BacktestTransition{
					Ts:          now,
					InstanceKey: res.InstanceKey,
					FromState:   tr.FromState,
					ToState:     tr.ToState,
					Values:      res.Windows,
				})
			}
		}
	}
	resp.Ticks = ticks
	return resp, nil
}
