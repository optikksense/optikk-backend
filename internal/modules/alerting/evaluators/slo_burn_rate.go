package evaluators

import "context"

// SLOBurnRate computes short/long error rate windows against the same root-span
// definition used by internal/modules/overview/slo. v1 rolls the whole rule up
// to one synthetic instance (group_by is accepted but currently treated as a
// single "*" key — frontend tag scoping remains).
type SLOBurnRate struct {
	Data DataSource
}

func (e *SLOBurnRate) Kind() string { return "slo_burn_rate" }

func (e *SLOBurnRate) Evaluate(ctx context.Context, rule Rule) ([]InstanceResult, error) {
	res := InstanceResult{
		InstanceKey: "*",
		Windows:     make(map[string]float64, len(rule.Windows)),
	}
	noDataAll := true
	for _, w := range rule.Windows {
		val, noData, err := e.Data.SLOErrorRate(ctx, rule.TeamID, rule.TargetService, w.Secs)
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

func (e *SLOBurnRate) EvaluateAt(ctx context.Context, rule Rule, fromMs, toMs int64) ([]InstanceResult, error) {
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
