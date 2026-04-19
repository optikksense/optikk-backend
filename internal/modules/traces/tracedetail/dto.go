package tracedetail

import "time"

// spanKindDurationRow is the scan target for GetSpanKindBreakdown.
// PctOfTrace is computed in the service layer.
type spanKindDurationRow struct {
	SpanKind    string  `ch:"span_kind"`
	TotalDuraMs float64 `ch:"total_duration_ms"`
	SpanCount   int64   `ch:"span_count"`
}

// criticalPathRow is the scan target for GetCriticalPath.
// The graph traversal algorithm runs in the service layer.
type criticalPathRow struct {
	SpanID        string  `ch:"span_id"`
	ParentSpanID  string  `ch:"parent_span_id"`
	OperationName string  `ch:"operation_name"`
	ServiceName   string  `ch:"service_name"`
	DurationMs    float64 `ch:"duration_ms"`
	StartNs       int64   `ch:"start_ns"`
	EndNs         int64   `ch:"end_ns"`
}

// errorPathRow is the scan target for GetErrorPath.
// The chain traversal algorithm runs in the service layer.
type errorPathRow struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	OperationName string    `ch:"operation_name"`
	ServiceName   string    `ch:"service_name"`
	Status        string    `ch:"status"`
	StatusMessage string    `ch:"status_message"`
	StartTime     time.Time `ch:"start_time"`
	DurationMs    float64   `ch:"duration_ms"`
}

// traceLogRow is the scan target for GetTraceLogs.
type traceLogRow struct {
	Timestamp         time.Time          `ch:"timestamp"`
	ObservedTimestamp uint64             `ch:"observed_timestamp"`
	SeverityText      string             `ch:"severity_text"`
	SeverityNumber    uint8              `ch:"severity_number"`
	Body              string             `ch:"body"`
	TraceID           string             `ch:"trace_id"`
	SpanID            string             `ch:"span_id"`
	TraceFlags        uint32             `ch:"trace_flags"`
	ServiceName       string             `ch:"service"`
	Host              string             `ch:"host"`
	Pod               string             `ch:"pod"`
	Container         string             `ch:"container"`
	Environment       string             `ch:"environment"`
	AttributesString  map[string]string  `ch:"attributes_string"`
	AttributesNumber  map[string]float64 `ch:"attributes_number"`
	AttributesBool    map[string]bool    `ch:"attributes_bool"`
	ScopeName         string             `ch:"scope_name"`
	ScopeVersion      string             `ch:"scope_version"`
}

// flamegraphRow is the scan target for GetFlamegraphData.
// Name, Level, and SelfTimeMs are computed in the service layer.
type flamegraphRow struct {
	SpanID        string  `ch:"span_id"`
	ParentSpanID  string  `ch:"parent_span_id"`
	OperationName string  `ch:"operation_name"`
	ServiceName   string  `ch:"service_name"`
	SpanKind      string  `ch:"span_kind"`
	DurationMs    float64 `ch:"duration_ms"`
	StartNs       int64   `ch:"start_ns"`
	HasError      bool    `ch:"has_error"`
}

// spanEventRow is the flattened shape the service layer returns — one entry
// per span event. Produced by the repository Go-side unpacking loop; no longer
// a direct CH scan target.
type spanEventRow struct {
	SpanID    string    `ch:"span_id"`
	TraceID   string    `ch:"trace_id"`
	Timestamp time.Time `ch:"timestamp"`
	EventJSON string    `ch:"event_json"`
}

// spanEventsRawRow is the actual CH scan target: one row per span, with its
// events array intact. repository.GetSpanEvents unpacks into []spanEventRow
// without CH doing an arrayJoin — fewer bytes on the wire, zero CH CPU for
// the fan-out.
type spanEventsRawRow struct {
	SpanID    string    `ch:"span_id"`
	TraceID   string    `ch:"trace_id"`
	Timestamp time.Time `ch:"timestamp"`
	Events    []string  `ch:"events"`
}

// exceptionRow is the scan target for the span exception fields query.
type exceptionRow struct {
	SpanID              string    `ch:"span_id"`
	TraceID             string    `ch:"trace_id"`
	Timestamp           time.Time `ch:"timestamp"`
	ExceptionType       string    `ch:"exception_type"`
	ExceptionMessage    string    `ch:"exception_message"`
	ExceptionStacktrace string    `ch:"exception_stacktrace"`
}

// spanAttributeRow is the scan target for GetSpanAttributes.
// Merged Attributes map and DBStatementNormalized are built in the service layer.
type spanAttributeRow struct {
	SpanID              string            `ch:"span_id"`
	TraceID             string            `ch:"trace_id"`
	OperationName       string            `ch:"operation_name"`
	ServiceName         string            `ch:"service_name"`
	AttributesString    map[string]string `ch:"attributes_string"`
	ResourceAttrs       map[string]string `ch:"resource_attributes"`
	ExceptionType       string            `ch:"exception_type"`
	ExceptionMessage    string            `ch:"exception_message"`
	ExceptionStacktrace string            `ch:"exception_stacktrace"`
	DBSystem            string            `ch:"db_system"`
	DBName              string            `ch:"db_name"`
	DBStatement         string            `ch:"db_statement"`
}
