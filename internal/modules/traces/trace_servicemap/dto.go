package trace_servicemap //nolint:revive,stylecheck

import "time"

// serviceMapSpanRow is the scan target for GetServiceMapSpans — just enough to
// build per-trace service nodes + client→server edges in the service layer.
type serviceMapSpanRow struct {
	SpanID       string  `ch:"span_id"`
	ParentSpanID string  `ch:"parent_span_id"`
	ServiceName  string  `ch:"service"`
	DurationMs   float64 `ch:"duration_ms"`
	HasError     bool    `ch:"has_error"`
}

// traceErrorRow is the scan target for GetTraceErrors — one row per error span.
type traceErrorRow struct {
	SpanID           string    `ch:"span_id"`
	ServiceName      string    `ch:"service"`
	OperationName    string    `ch:"operation_name"`
	ExceptionType    string    `ch:"exception_type"`
	ExceptionMessage string    `ch:"exception_message"`
	StatusMessage    string    `ch:"status_message"`
	StartTime        time.Time `ch:"start_time"`
	DurationMs       float64   `ch:"duration_ms"`
}
