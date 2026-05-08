package detail

import "time"

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
// AttributesJSON is the toJSONString-serialized attributes column; the service
// layer flattens it into a Map(String, String) shape and merges in resource
// attributes (currently always empty — spans have no separate resource store).
type spanAttributeRow struct {
	SpanID              string `ch:"span_id"`
	TraceID             string `ch:"trace_id"`
	OperationName       string `ch:"operation_name"`
	ServiceName         string `ch:"service"`
	AttributesJSON      string `ch:"attributes_json"`
	ExceptionType       string `ch:"exception_type"`
	ExceptionMessage    string `ch:"exception_message"`
	ExceptionStacktrace string `ch:"exception_stacktrace"`
	DBSystem            string `ch:"db_system"`
	DBName              string `ch:"db_name"`
	DBStatement         string `ch:"db_statement"`
	Links               string `ch:"links"`
}
