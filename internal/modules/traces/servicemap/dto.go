package servicemap

import (
	"time"
)

type serviceMapSpanRow struct {
	SpanID       string  `ch:"span_id"`
	ParentSpanID string  `ch:"parent_span_id"`
	ServiceName  string  `ch:"service"`
	DurationMs   float64 `ch:"duration_ms"`
	HasError     bool    `ch:"has_error"`
}

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
