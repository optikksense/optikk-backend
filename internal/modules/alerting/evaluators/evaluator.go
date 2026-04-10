// Package evaluators holds per-condition-type Evaluator implementations used by
// the alerting module. Keeping them behind a small interface lets the engine
// stay generic (metric_threshold, log_count, absence etc. slot in as new
// structs, zero schema churn).
package evaluators

import "context"

// InstanceResult is the output of Evaluator.Evaluate for a single instance.
type InstanceResult struct {
	InstanceKey string
	GroupValues map[string]string
	Windows     map[string]float64 // "short": 0.12, "long": 0.08
	NoData      bool
}

// Rule is the minimal rule projection the evaluator needs. The alerting module
// passes its own models.Rule in via an adapter so this package has no circular
// dependency back into the parent.
type Rule struct {
	ID            int64
	TeamID        int64
	ConditionType string
	TargetService string
	GroupBy       []string
	Windows       []Window
}

// Window mirrors alerting.Window.
type Window struct {
	Name string
	Secs int64
}

// DataSource is the narrow contract the evaluators need from the alerting
// repository. Decouples evaluators from the concrete ClickHouse repo.
type DataSource interface {
	SLOErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error)
	ServiceErrorRate(ctx context.Context, teamID int64, serviceName string, windowSecs int64) (float64, bool, error)
	ErrorRateHistorical(ctx context.Context, teamID int64, serviceName string, fromMs, toMs, windowSecs int64) (float64, bool, error)
}

// Evaluator is the per-condition-type contract.
type Evaluator interface {
	Kind() string
	Evaluate(ctx context.Context, rule Rule) ([]InstanceResult, error)
	EvaluateAt(ctx context.Context, rule Rule, fromMs, toMs int64) ([]InstanceResult, error)
}

// Registry wires a fixed set of evaluators at boot time.
type Registry struct {
	byKind map[string]Evaluator
}

func NewRegistry(evals ...Evaluator) *Registry {
	m := make(map[string]Evaluator, len(evals))
	for _, e := range evals {
		m[e.Kind()] = e
	}
	return &Registry{byKind: m}
}

func (r *Registry) Get(kind string) (Evaluator, bool) {
	e, ok := r.byKind[kind]
	return e, ok
}
