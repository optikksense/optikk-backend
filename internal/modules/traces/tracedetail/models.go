package tracedetail

import "time"

type SpanEvent struct {
	SpanID     string    `json:"span_id"     ch:"span_id"`
	TraceID    string    `json:"trace_id"    ch:"trace_id"`
	EventName  string    `json:"event_name"  ch:"event_name"`
	Timestamp  time.Time `json:"timestamp"   ch:"timestamp"`
	Attributes string    `json:"attributes"`
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

	// Parsed OTLP span links pointing at other traces/spans (O13).
	Links []SpanLink `json:"links,omitempty"`
}

// SpanLink is a typed OpenTelemetry span link. See spans schema `links` column.
type SpanLink struct {
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
	TraceState string            `json:"trace_state,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// TraceLog is the JSON response model for a log entry associated with a trace.
type TraceLog struct {
	Timestamp         uint64             `json:"timestamp"`
	ObservedTimestamp uint64             `json:"observed_timestamp,omitempty"`
	SeverityText      string             `json:"severity_text"`
	SeverityNumber    uint8              `json:"severity_number,omitempty"`
	Body              string             `json:"body"`
	TraceID           string             `json:"trace_id"`
	SpanID            string             `json:"span_id"`
	TraceFlags        uint32             `json:"trace_flags,omitempty"`
	ServiceName       string             `json:"service_name"`
	Host              string             `json:"host"`
	Pod               string             `json:"pod"`
	Container         string             `json:"container"`
	Environment       string             `json:"environment"`
	AttributesString  map[string]string  `json:"attributes_string,omitempty"`
	AttributesNumber  map[string]float64 `json:"attributes_number,omitempty"`
	AttributesBool    map[string]bool    `json:"attributes_bool,omitempty"`
	ScopeName         string             `json:"scope_name,omitempty"`
	ScopeVersion      string             `json:"scope_version,omitempty"`
}

// TraceLogsResponse wraps the trace logs with a speculative flag.
type TraceLogsResponse struct {
	Logs          []TraceLog `json:"logs"`
	IsSpeculative bool       `json:"is_speculative"`
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

