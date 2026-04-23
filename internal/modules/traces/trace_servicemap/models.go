// Package trace_servicemap exposes per-trace aggregation endpoints (service
// dependency graph + error groups) split out of the legacy tracedetail module.
package trace_servicemap //nolint:revive,stylecheck // underscore matches sibling package naming for traces/*

import "time"

// ServiceMapNode is one service in a per-trace dependency graph.
type ServiceMapNode struct {
	Service    string  `json:"service"`
	SpanCount  int     `json:"span_count"`
	ErrorCount int     `json:"error_count"`
	TotalMs    float64 `json:"total_ms"`
}

// ServiceMapEdge is a directed (client→server) call between two services inside a single trace.
type ServiceMapEdge struct {
	From       string  `json:"from"`
	To         string  `json:"to"`
	CallCount  int     `json:"call_count"`
	ErrorCount int     `json:"error_count"`
	TotalMs    float64 `json:"total_ms"`
}

// ServiceMapResponse wraps the per-trace service map payload.
type ServiceMapResponse struct {
	Nodes []ServiceMapNode `json:"nodes"`
	Edges []ServiceMapEdge `json:"edges"`
}

// TraceErrorGroup aggregates error spans sharing the same exception_type.
type TraceErrorGroup struct {
	ExceptionType string           `json:"exception_type"`
	Count         int              `json:"count"`
	Spans         []TraceErrorSpan `json:"spans"`
}

// TraceErrorSpan is a single error span in a trace-level errors aggregation.
type TraceErrorSpan struct {
	SpanID           string    `json:"span_id"`
	ServiceName      string    `json:"service_name"`
	OperationName    string    `json:"operation_name"`
	ExceptionMessage string    `json:"exception_message,omitempty"`
	StatusMessage    string    `json:"status_message,omitempty"`
	StartTime        time.Time `json:"start_time"`
	DurationMs       float64   `json:"duration_ms"`
}
