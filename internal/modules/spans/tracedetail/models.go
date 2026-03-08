package tracedetail

import "time"

// SpanEvent represents a single event attached to a span (e.g. an exception).
type SpanEvent struct {
	SpanID     string    `json:"spanId"`
	TraceID    string    `json:"traceId"`
	EventName  string    `json:"eventName"`
	Timestamp  time.Time `json:"timestamp"`
	Attributes string    `json:"attributes"`
}

// SpanKindDuration represents the total duration consumed by a span.kind bucket.
type SpanKindDuration struct {
	SpanKind    string  `json:"spanKind"`
	TotalDuraMs float64 `json:"totalDurationMs"`
	SpanCount   int64   `json:"spanCount"`
	PctOfTrace  float64 `json:"pctOfTrace"`
}

// CriticalPathSpan is a span_id that lies on the critical (longest root→leaf) path.
type CriticalPathSpan struct {
	SpanID        string  `json:"spanId"`
	OperationName string  `json:"operationName"`
	ServiceName   string  `json:"serviceName"`
	DurationMs    float64 `json:"durationMs"`
}

// SpanSelfTime breaks down a span's self time vs total time.
type SpanSelfTime struct {
	SpanID        string  `json:"spanId"`
	OperationName string  `json:"operationName"`
	TotalDuraMs   float64 `json:"totalDurationMs"`
	SelfTimeMs    float64 `json:"selfTimeMs"`
	ChildTimeMs   float64 `json:"childTimeMs"`
}

// ErrorPathSpan is a span on the error propagation chain (root → error leaf).
type ErrorPathSpan struct {
	SpanID        string    `json:"spanId"`
	ParentSpanID  string    `json:"parentSpanId"`
	OperationName string    `json:"operationName"`
	ServiceName   string    `json:"serviceName"`
	Status        string    `json:"status"`
	StatusMessage string    `json:"statusMessage"`
	StartTime     time.Time `json:"startTime"`
	DurationMs    float64   `json:"durationMs"`
}

// SpanAttributes is the full attribute map for a single span.
type SpanAttributes struct {
	SpanID           string            `json:"spanId"`
	TraceID          string            `json:"traceId"`
	OperationName    string            `json:"operationName"`
	ServiceName      string            `json:"serviceName"`
	AttributesString map[string]string `json:"attributesString"`
	// Resource attributes (service.name, host.name, etc.)
	ResourceAttrs map[string]string `json:"resourceAttributes"`
	// Exception fields, if any
	ExceptionType       string `json:"exceptionType,omitempty"`
	ExceptionMessage    string `json:"exceptionMessage,omitempty"`
	ExceptionStacktrace string `json:"exceptionStacktrace,omitempty"`
	// DB fields
	DBSystem              string `json:"dbSystem,omitempty"`
	DBName                string `json:"dbName,omitempty"`
	DBStatement           string `json:"dbStatement,omitempty"`
	DBStatementNormalized string `json:"dbStatementNormalized,omitempty"`

	// Merged attributes map (attributesString + resourceAttrs merged for convenience)
	Attributes map[string]string `json:"attributes,omitempty"`
}

// RelatedTrace is a summary of a trace with the same service+operation.
type RelatedTrace struct {
	TraceID       string  `json:"traceId"`
	SpanID        string  `json:"spanId"`
	OperationName string  `json:"operationName"`
	ServiceName   string  `json:"serviceName"`
	DurationMs    float64 `json:"durationMs"`
	Status        string  `json:"status"`
	StartTime     time.Time `json:"startTime"`
}
