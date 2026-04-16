package runs

import "time"

type alertRow struct {
	ID            int64
	Name          string
	ConditionType string
	RuleState     string
	TargetRef     string
}

type logRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	SeverityText string    `ch:"severity_text"`
	Body         string    `ch:"body"`
	ServiceName  string    `ch:"service_name"`
	TraceID      string    `ch:"trace_id"`
	SpanID       string    `ch:"span_id"`
}

type facetDefinition struct {
	key   string
	label string
	expr  string
}
