package evaluators

import "context"

type AIErrorRate struct {
	Data DataSource
}

func (e *AIErrorRate) Kind() string { return "ai_error_rate" }

func (e *AIErrorRate) Evaluate(ctx context.Context, rule Rule) ([]InstanceResult, error) {
	return e.evaluate(ctx, rule, 0, 0, false)
}

func (e *AIErrorRate) EvaluateAt(ctx context.Context, rule Rule, fromMs, toMs int64) ([]InstanceResult, error) {
	return e.evaluate(ctx, rule, fromMs, toMs, true)
}

func (e *AIErrorRate) evaluate(ctx context.Context, rule Rule, fromMs, toMs int64, historical bool) ([]InstanceResult, error) {
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
			value, noData, err = e.Data.AIMetricHistorical(ctx, rule.TeamID, rule.TargetRef, "error_rate_pct", fromMs, toMs, window.Secs)
		} else {
			value, noData, err = e.Data.AIMetric(ctx, rule.TeamID, rule.TargetRef, "error_rate_pct", window.Secs)
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
