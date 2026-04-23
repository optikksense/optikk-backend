package tracedetail

import "time"

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

// spanEventRow is the scan target for the events ARRAY JOIN query.
type spanEventRow struct {
	SpanID    string    `ch:"span_id"`
	TraceID   string    `ch:"trace_id"`
	Timestamp time.Time `ch:"timestamp"`
	EventJSON string    `ch:"event_json"`
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

// spanEventCombinedRow fetches events (as array) and exception fields in one scan.
type spanEventCombinedRow struct {
	SpanID              string    `ch:"span_id"`
	TraceID             string    `ch:"trace_id"`
	Timestamp           time.Time `ch:"timestamp"`
	Events              []string  `ch:"events"`
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
	Links               string            `ch:"links"`
}
