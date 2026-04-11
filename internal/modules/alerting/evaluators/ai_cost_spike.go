package evaluators

import "context"

type AICostSpike struct {
	Data DataSource
}

func (e *AICostSpike) Kind() string { return "ai_cost_spike" }

func (e *AICostSpike) Evaluate(ctx context.Context, rule Rule) ([]InstanceResult, error) {
	return e.evaluate(ctx, rule, 0, 0, false)
}

func (e *AICostSpike) EvaluateAt(ctx context.Context, rule Rule, fromMs, toMs int64) ([]InstanceResult, error) {
	return e.evaluate(ctx, rule, fromMs, toMs, true)
}

func (e *AICostSpike) evaluate(ctx context.Context, rule Rule, fromMs, toMs int64, historical bool) ([]InstanceResult, error) {
	res := InstanceResult{
		InstanceKey: "*",
		Windows:     make(map[string]float64, len(rule.Windows)),
	}
	noDataAll := true
	for _, window := range rule.Windows {
		var (
			value  float64
			noData bool
			err    error
		)
		if historical {
			value, noData, err = e.Data.AIMetricHistorical(ctx, rule.TeamID, rule.TargetRef, "cost_usd", fromMs, toMs, window.Secs)
		} else {
			value, noData, err = e.Data.AIMetric(ctx, rule.TeamID, rule.TargetRef, "cost_usd", window.Secs)
		}
		if err != nil {
			return nil, err
		}
		res.Windows[window.Name] = value
		if !noData {
			noDataAll = false
		}
	}
	res.NoData = noDataAll
	return []InstanceResult{res}, nil
}
