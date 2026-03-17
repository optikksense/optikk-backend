package tracedetail

import "time"

type SpanEvent struct {
	SpanID     string    `json:"span_id"     ch:"span_id"`
	TraceID    string    `json:"trace_id"    ch:"trace_id"`
	EventName  string    `json:"event_name"  ch:"event_name"`
	Timestamp  time.Time `json:"timestamp"   ch:"timestamp"`
	Attributes string    `json:"attributes"`
}

type SpanKindDuration struct {
	SpanKind    string  `json:"span_kind"         ch:"span_kind"`
	TotalDuraMs float64 `json:"total_duration_ms" ch:"total_duration_ms"`
	SpanCount   int64   `json:"span_count"        ch:"span_count"`
	PctOfTrace  float64 `json:"pct_of_trace"`
}

type CriticalPathSpan struct {
	SpanID        string  `json:"span_id"        ch:"span_id"`
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ServiceName   string  `json:"service_name"   ch:"service_name"`
	DurationMs    float64 `json:"duration_ms"    ch:"duration_ms"`
}

// SpanSelfTime breaks down a span's self time vs total time.
type SpanSelfTime struct {
	SpanID        string  `json:"span_id"           ch:"span_id"`
	OperationName string  `json:"operation_name"    ch:"operation_name"`
	TotalDuraMs   float64 `json:"total_duration_ms" ch:"total_duration_ms"`
	SelfTimeMs    float64 `json:"self_time_ms"      ch:"self_time_ms"`
	ChildTimeMs   float64 `json:"child_time_ms"     ch:"child_time_ms"`
}

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

type SpanAttributes struct {
	SpanID           string            `json:"span_id"`
	TraceID          string            `json:"trace_id"`
	OperationName    string            `json:"operation_name"`
	ServiceName      string            `json:"service_name"`
	AttributesString map[string]string `json:"attributes_string"`
	// Resource attributes (service.name, host.name, etc.)
	ResourceAttrs map[string]string `json:"resource_attributes"`
	// Exception fields, if any
	ExceptionType       string `json:"exception_type,omitempty"`
	ExceptionMessage    string `json:"exception_message,omitempty"`
	ExceptionStacktrace string `json:"exception_stacktrace,omitempty"`
	// DB fields
	DBSystem              string `json:"db_system,omitempty"`
	DBName                string `json:"db_name,omitempty"`
	DBStatement           string `json:"db_statement,omitempty"`
	DBStatementNormalized string `json:"db_statement_normalized,omitempty"`

	// Merged attributes map (attributesString + resourceAttrs merged for convenience)
	Attributes map[string]string `json:"attributes,omitempty"`
}

// FlamegraphFrame represents a single frame in a flamegraph visualization.
// Frames are ordered depth-first with self-time computed.
type FlamegraphFrame struct {
	SpanID     string  `json:"span_id"`
	Name       string  `json:"name"` // "service :: operation"
	Service    string  `json:"service"`
	Operation  string  `json:"operation"`
	DurationMs float64 `json:"duration_ms"`
	SelfTimeMs float64 `json:"self_time_ms"`
	Level      int     `json:"level"`
	SpanKind   string  `json:"span_kind"`
	HasError   bool    `json:"has_error"`
}

type RelatedTrace struct {
	TraceID       string    `json:"trace_id"       ch:"trace_id"`
	SpanID        string    `json:"span_id"        ch:"span_id"`
	OperationName string    `json:"operation_name" ch:"operation_name"`
	ServiceName   string    `json:"service_name"   ch:"service_name"`
	DurationMs    float64   `json:"duration_ms"    ch:"duration_ms"`
	Status        string    `json:"status"         ch:"status"`
	StartTime     time.Time `json:"start_time"     ch:"start_time"`
}
