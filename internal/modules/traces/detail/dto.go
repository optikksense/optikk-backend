package detail

import (
	"time"
)

type spanEventRow struct {
	SpanID    string    `ch:"span_id"`
	TraceID   string    `ch:"trace_id"`
	Timestamp time.Time `ch:"timestamp"`
	EventJSON string    `ch:"event_json"`
}

type exceptionRow struct {
	SpanID              string    `ch:"span_id"`
	TraceID             string    `ch:"trace_id"`
	Timestamp           time.Time `ch:"timestamp"`
	ExceptionType       string    `ch:"exception_type"`
	ExceptionMessage    string    `ch:"exception_message"`
	ExceptionStacktrace string    `ch:"exception_stacktrace"`
}

type spanEventCombinedRow struct {
	SpanID              string    `ch:"span_id"`
	TraceID             string    `ch:"trace_id"`
	Timestamp           time.Time `ch:"timestamp"`
	Events              []string  `ch:"events"`
	ExceptionType       string    `ch:"exception_type"`
	ExceptionMessage    string    `ch:"exception_message"`
	ExceptionStacktrace string    `ch:"exception_stacktrace"`
}

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
