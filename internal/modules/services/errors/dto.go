package errors

import "time"

// Raw ClickHouse row structs scanned directly from query results.
// All Go-side derivations (rate, latency, status-bucket → code, hash) live in service.go.

type rawServiceRateRow struct {
	ServiceName   string  `ch:"service"`
	TsBucket      uint32  `ch:"ts_bucket"`
	RequestCount  uint64  `ch:"request_count"`
	ErrorCount    uint64  `ch:"error_count"`
	DurationMsSum float64 `ch:"duration_ms_sum"`
}

type rawServiceErrorRow struct {
	ServiceName string `ch:"service"`
	TsBucket    uint32 `ch:"ts_bucket"`
	ErrorCount  uint64 `ch:"error_count"`
}

type rawErrorGroupRow struct {
	ServiceName      string    `ch:"service"`
	OperationName    string    `ch:"operation_name"`
	StatusMessage    string    `ch:"status_message"`
	HTTPStatusBucket string    `ch:"http_status_bucket"`
	ErrorCount       uint64    `ch:"error_count"`
	LastOccurrence   time.Time `ch:"last_occurrence"`
	FirstOccurrence  time.Time `ch:"first_occurrence"`
	SampleTraceID    string    `ch:"sample_trace_id"`
}

type rawErrorGroupDetailRow struct {
	ServiceName     string    `ch:"service"`
	OperationName   string    `ch:"operation_name"`
	StatusMessage   string    `ch:"status_message"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      uint64    `ch:"error_count"`
	LastOccurrence  time.Time `ch:"last_occurrence"`
	FirstOccurrence time.Time `ch:"first_occurrence"`
	SampleTraceID   string    `ch:"sample_trace_id"`
	ExceptionType   string    `ch:"exception_type"`
	StackTrace      string    `ch:"stack_trace"`
}

type rawErrorGroupTraceRow struct {
	TraceID    string    `ch:"trace_id"`
	SpanID     string    `ch:"span_id"`
	Timestamp  time.Time `ch:"timestamp"`
	DurationMs float64   `ch:"duration_ms"`
	StatusCode string    `ch:"status_code"`
}

type rawTimeBucketCountRow struct {
	TsBucket uint32 `ch:"ts_bucket"`
	Count    uint64 `ch:"count"`
}

type rawExceptionRateRow struct {
	TsBucket      uint32 `ch:"ts_bucket"`
	ExceptionType string `ch:"exception_type"`
	Count         uint64 `ch:"event_count"`
}

type rawErrorHotspotRow struct {
	ServiceName   string `ch:"service"`
	OperationName string `ch:"operation_name"`
	ErrorCount    uint64 `ch:"error_count"`
	TotalCount    uint64 `ch:"total_count"`
}

type rawHTTP5xxRow struct {
	HTTPRoute   string `ch:"http_route"`
	ServiceName string `ch:"service"`
	Count       uint64 `ch:"count_5xx"`
}

type rawErrorFingerprintRow struct {
	Fingerprint   string    `ch:"fingerprint"`
	ServiceName   string    `ch:"service"`
	OperationName string    `ch:"operation_name"`
	ExceptionType string    `ch:"exception_type"`
	StatusMessage string    `ch:"status_message"`
	FirstSeen     time.Time `ch:"first_seen"`
	LastSeen      time.Time `ch:"last_seen"`
	Count         uint64    `ch:"cnt"`
	SampleTraceID string    `ch:"sample_trace_id"`
}

type rawFingerprintTrendRow struct {
	TsBucket uint32 `ch:"ts_bucket"`
	Count    uint64 `ch:"cnt"`
}
