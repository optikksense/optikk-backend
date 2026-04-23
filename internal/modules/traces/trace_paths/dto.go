package trace_paths //nolint:revive,stylecheck

import "time"

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
