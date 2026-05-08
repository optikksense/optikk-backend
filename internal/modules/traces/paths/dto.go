package paths

import "time"

// criticalPathRow is the scan target for GetCriticalPath. Native CH types
// (timestamp + duration_nano) are scanned; the service layer converts to
// nanoseconds for the longest-path graph algorithm.
type criticalPathRow struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	OperationName string    `ch:"operation_name"`
	ServiceName   string    `ch:"service"`
	DurationMs    float64   `ch:"duration_ms"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationNano  int64     `ch:"duration_nano"`
}

// errorPathRow is the scan target for GetErrorPath.
// The chain traversal algorithm runs in the service layer.
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
