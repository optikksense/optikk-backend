package alerting

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/modules/alerting/evaluators"
)

// RunBacktest replays the rule across [fromMs,toMs] at stepMs granularity using
// the historical evaluator path. No state writes, no dispatches.
func RunBacktest(ctx context.Context, reg *evaluators.Registry, rule *Rule, fromMs, toMs, stepMs int64) (BacktestResponse, error) {
	if stepMs <= 0 {
		stepMs = 60_000
	}
	eval, ok := reg.Get(rule.ConditionType)
	if !ok {
		return BacktestResponse{}, ErrUnsupportedCondition
	}

	var resp BacktestResponse
	state := make(InstancesMap)
	ticks := 0
	for t := fromMs + stepMs; t <= toMs; t += stepMs {
		ticks++
		results, err := eval.EvaluateAt(ctx, adaptRule(rule), t-stepMs, t)
		if err != nil {
			return BacktestResponse{}, err
		}
		for _, res := range results {
			inst, ok := state[res.InstanceKey]
			if !ok {
				inst = &Instance{InstanceKey: res.InstanceKey, State: StateOK, GroupValues: res.GroupValues}
				state[res.InstanceKey] = inst
			}
			now := time.UnixMilli(t)
			tr := Decide(rule, inst, res.Windows, res.NoData, now)
			inst.Values = res.Windows
			if tr.Changed {
				inst.State = tr.ToState
				resp.Transitions = append(resp.Transitions, BacktestTransition{
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

// adaptRule projects alerting.Rule to the evaluators.Rule shape.
func adaptRule(r *Rule) evaluators.Rule {
	ws := make([]evaluators.Window, len(r.Windows))
	for i, w := range r.Windows {
		ws[i] = evaluators.Window{Name: w.Name, Secs: w.Secs}
	}
	return evaluators.Rule{
		ID:            r.ID,
		TeamID:        r.TeamID,
		ConditionType: r.ConditionType,
		TargetRef:     r.TargetRef,
		TargetService: targetServiceFromRef(r.TargetRef),
		GroupBy:       r.GroupBy,
		Windows:       ws,
	}
}
