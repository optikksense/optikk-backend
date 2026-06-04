package paths

import (
	"time"
)

type criticalPathRow struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	OperationName string    `ch:"operation_name"`
	ServiceName   string    `ch:"service"`
	DurationMs    float64   `ch:"duration_ms"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationNano  uint64    `ch:"duration_nano"`
}

type errorPathRow struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	OperationName string    `ch:"operation_name"`
	ServiceName   string    `ch:"service"`
	Status        string    `ch:"status"`
	StatusMessage string    `ch:"status_message"`
	StartTime     time.Time `ch:"start_time"`
	DurationMs    float64   `ch:"duration_ms"`
}
