package evaluators

import "context"

type AIQualityDrop struct {
	Data DataSource
}

func (e *AIQualityDrop) Kind() string { return "ai_quality_drop" }

func (e *AIQualityDrop) Evaluate(ctx context.Context, rule Rule) ([]InstanceResult, error) {
	return e.evaluate(ctx, rule, 0, 0, false)
}

func (e *AIQualityDrop) EvaluateAt(ctx context.Context, rule Rule, fromMs, toMs int64) ([]InstanceResult, error) {
	return e.evaluate(ctx, rule, fromMs, toMs, true)
}

func (e *AIQualityDrop) evaluate(ctx context.Context, rule Rule, fromMs, toMs int64, historical bool) ([]InstanceResult, error) {
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
			value, noData, err = e.Data.AIMetricHistorical(ctx, rule.TeamID, rule.TargetRef, "quality_score", fromMs, toMs, window.Secs)
		} else {
			value, noData, err = e.Data.AIMetric(ctx, rule.TeamID, rule.TargetRef, "quality_score", window.Secs)
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
