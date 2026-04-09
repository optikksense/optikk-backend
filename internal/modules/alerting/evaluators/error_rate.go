package evaluators

import "context"

// ErrorRate is the generic service error-rate evaluator. In v1 it shares the
// same root-span definition as the SLO burn-rate evaluator but is kept as a
// separate type so each can evolve independently.
type ErrorRate struct {
	Data DataSource
}

func (e *ErrorRate) Kind() string { return "error_rate" }

func (e *ErrorRate) Evaluate(ctx context.Context, rule Rule) ([]InstanceResult, error) {
	res := InstanceResult{
		InstanceKey: "*",
		Windows:     make(map[string]float64, len(rule.Windows)),
	}
	noDataAll := true
	for _, w := range rule.Windows {
		val, noData, err := e.Data.ServiceErrorRate(ctx, rule.TeamID, rule.TargetService, w.Secs)
		if err != nil {
			return nil, err
		}
		res.Windows[w.Name] = val
		if !noData {
			noDataAll = false
		}
	}
	res.NoData = noDataAll
	return []InstanceResult{res}, nil
}

func (e *ErrorRate) EvaluateAt(ctx context.Context, rule Rule, fromMs, toMs int64) ([]InstanceResult, error) {
	res := InstanceResult{
		InstanceKey: "*",
		Windows:     make(map[string]float64, len(rule.Windows)),
	}
	noDataAll := true
	for _, w := range rule.Windows {
		val, noData, err := e.Data.ErrorRateHistorical(ctx, rule.TeamID, rule.TargetService, fromMs, toMs, w.Secs)
		if err != nil {
			return nil, err
		}
		res.Windows[w.Name] = val
		if !noData {
			noDataAll = false
		}
	}
	res.NoData = noDataAll
	return []InstanceResult{res}, nil
}
