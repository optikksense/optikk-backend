// Package trace_paths exposes trace chain analysis endpoints (critical-path
// and error-path) split out of the legacy tracedetail module.
package trace_paths //nolint:revive,stylecheck

import "time"

// CriticalPathSpan is one hop in the longest dependency chain of a trace.
type CriticalPathSpan struct {
	SpanID        string  `json:"span_id"        ch:"span_id"`
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ServiceName   string  `json:"service_name"   ch:"service_name"`
	DurationMs    float64 `json:"duration_ms"    ch:"duration_ms"`
}

// ErrorPathSpan is one hop in the error ancestry chain (leaf error → root).
type ErrorPathSpan struct {
	SpanID        string    `json:"span_id"        ch:"span_id"`
	ParentSpanID  string    `json:"parent_span_id" ch:"parent_span_id"`
	OperationName string    `json:"operation_name" ch:"operation_name"`
	ServiceName   string    `json:"service_name"   ch:"service_name"`
	Status        string    `json:"status"         ch:"status"`
	StatusMessage string    `json:"status_message" ch:"status_message"`
	StartTime     time.Time `json:"start_time"     ch:"start_time"`
	DurationMs    float64   `json:"duration_ms"    ch:"duration_ms"`
}
