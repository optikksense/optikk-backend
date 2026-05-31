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
	GroupID          string    `ch:"error_group_id"`
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
	GroupID         string    `ch:"error_group_id"`
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

type rawErrorHotspotRow struct {
	ServiceName   string `ch:"service"`
	OperationName string `ch:"operation_name"`
	ErrorCount    uint64 `ch:"error_count"`
	TotalCount    uint64 `ch:"total_count"`
}


type ErrorGroupsCursor struct {
	ErrorCount uint64 `json:"cnt"`
	GroupID    string `json:"id"`
}

func (c ErrorGroupsCursor) IsZero() bool {
	return c.ErrorCount == 0 && c.GroupID == ""
}

type PageInfo struct {
	HasMore    bool   `json:"hasMore"`
	NextCursor string `json:"nextCursor,omitempty"`
	Limit      int    `json:"limit"`
}

type PaginatedErrorGroups struct {
	Results  []ErrorGroup `json:"results"`
	PageInfo PageInfo     `json:"pageInfo"`
}
