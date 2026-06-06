package errors

import "time"

// Raw ClickHouse row structs scanned directly from query results.
// All Go-side derivations live in service.go.

type rawServiceRateRow struct {
	ServiceName   string    `ch:"service"`
	BucketAt      time.Time `ch:"bucket_at"`
	RequestCount  uint64    `ch:"request_count"`
	ErrorCount    uint64    `ch:"error_count"`
	DurationMsSum float64   `ch:"duration_ms_sum"`
}

type rawServiceErrorRow struct {
	ServiceName string    `ch:"service"`
	BucketAt    time.Time `ch:"bucket_at"`
	ErrorCount  uint64    `ch:"error_count"`
}

type rawErrorGroupRow struct {
	GroupID          string    `ch:"error_group_id"`
	ServiceName      string    `ch:"service"`
	OperationName    string    `ch:"operation_name"`
	HTTPStatusBucket string    `ch:"http_status_bucket"`
	ErrorCount       uint64    `ch:"error_count"`
	LastOccurrence   time.Time `ch:"last_occurrence"`
	FirstOccurrence  time.Time `ch:"first_occurrence"`
}

// rawErrorGroupSampleRow carries the exemplar status message and trace_id
// resolved from raw spans.
type rawErrorGroupSampleRow struct {
	GroupID       string `ch:"error_group_id"`
	StatusMessage string `ch:"status_message"`
	SampleTraceID string `ch:"sample_trace_id"`
}

type rawErrorGroupDetailRow struct {
	GroupID         string    `ch:"error_group_id"`
	ServiceName     string    `ch:"service"`
	OperationName   string    `ch:"operation_name"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      uint64    `ch:"error_count"`
	LastOccurrence  time.Time `ch:"last_occurrence"`
	FirstOccurrence time.Time `ch:"first_occurrence"`
	ExceptionType   string    `ch:"exception_type"`
}

type rawErrorGroupTraceRow struct {
	TraceID    string    `ch:"trace_id"`
	SpanID     string    `ch:"span_id"`
	Timestamp  time.Time `ch:"timestamp"`
	DurationMs float64   `ch:"duration_ms"`
	StatusCode string    `ch:"status_code"`
}

// rawErrorLatestOccurrenceRow is the single most recent error span of a group,
// scanned from the raw spans table.
type rawErrorLatestOccurrenceRow struct {
	TraceID          string    `ch:"trace_id"`
	SpanID           string    `ch:"span_id"`
	Timestamp        time.Time `ch:"timestamp"`
	DurationMs       float64   `ch:"duration_ms"`
	ExceptionMessage string    `ch:"exception_message"`
	StackTrace       string    `ch:"exception_stacktrace"`
	HTTPMethod       string    `ch:"http_method"`
	HTTPRoute        string    `ch:"http_route"`
	HTTPStatusCode   string    `ch:"response_status_code"`
	ServiceVersion   string    `ch:"service_version"`
	Environment      string    `ch:"environment"`
	Pod              string    `ch:"pod"`
	Host             string    `ch:"host"`
}

// rawErrorFacetRow is one bucket of a single facet dimension (e.g. one pod).
type rawErrorFacetRow struct {
	Value string `ch:"value"`
	Count uint64 `ch:"count"`
}

type rawTimeBucketCountRow struct {
	BucketAt time.Time `ch:"bucket_at"`
	Count    uint64    `ch:"count"`
}

type rawErrorHotspotRow struct {
	ServiceName   string `ch:"service"`
	OperationName string `ch:"operation_name"`
	GroupID       string `ch:"error_group_id"`
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

type ErrorTracesCursor struct {
	Timestamp time.Time `json:"ts"`
	SpanID    string    `json:"sid"`
}

func (c ErrorTracesCursor) IsZero() bool {
	return c.Timestamp.IsZero() && c.SpanID == ""
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
