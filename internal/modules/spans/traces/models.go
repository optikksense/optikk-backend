package traces

import "time"

type Trace struct {
	SpanID         string    `json:"span_id"`
	TraceID        string    `json:"trace_id"`
	ServiceName    string    `json:"service_name"`
	OperationName  string    `json:"operation_name"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	DurationMs     float64   `json:"duration_ms"`
	Status         string    `json:"status"`
	StatusMessage  string    `json:"status_message"`
	HTTPMethod     string    `json:"http_method"`
	HTTPStatusCode int       `json:"http_status_code"`
	// ParentSpanID and SpanKind are populated only in span-level search (SearchMode="all").
	ParentSpanID string `json:"parent_span_id,omitempty"`
	SpanKind     string `json:"span_kind,omitempty"`
}

type Span struct {
	SpanID         string    `json:"span_id"`
	ParentSpanID   string    `json:"parent_span_id"`
	TraceID        string    `json:"trace_id"`
	OperationName  string    `json:"operation_name"`
	ServiceName    string    `json:"service_name"`
	SpanKind       string    `json:"span_kind"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	DurationMs     float64   `json:"duration_ms"`
	Status         string    `json:"status"`
	StatusMessage  string    `json:"status_message"`
	HTTPMethod     string    `json:"http_method"`
	HTTPURL        string    `json:"http_url"`
	HTTPStatusCode int       `json:"http_status_code"`
	Host           string    `json:"host"`
	Pod            string    `json:"pod"`
	Attributes     string    `json:"attributes"`
}

type TraceFilters struct {
	TeamID      int64    `json:"teamId"`
	StartMs     int64    `json:"startMs"`
	EndMs       int64    `json:"endMs"`
	Services    []string `json:"services"`
	Status      string   `json:"status"`
	SearchText  string   `json:"searchText"`
	MinDuration string   `json:"minDuration"`
	MaxDuration string   `json:"maxDuration"`
	TraceID     string   `json:"traceId"`
	Operation   string   `json:"operation"`
	HTTPMethod  string   `json:"httpMethod"`
	HTTPStatus  string   `json:"httpStatus"`
	// SearchMode: "root" (default) filters to root spans only; "all" searches all spans.
	SearchMode string `json:"searchMode"`
	// SpanKind filters by span kind (SERVER, CLIENT, INTERNAL, PRODUCER, CONSUMER).
	SpanKind string `json:"spanKind"`
	// SpanName filters by exact span name (vs Operation which uses LIKE).
	SpanName string `json:"spanName"`
	// AttributeFilters allows arbitrary span attribute filtering.
	// e.g. ?attr.db.name=mydb or ?attr_contains.db.statement=SELECT
	AttributeFilters []SpanAttributeFilter `json:"attributeFilters,omitempty"`
}

type SpanAttributeFilter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	// Op: "eq" (default), "neq", "contains", "regex"
	Op string `json:"op"`
}

type TraceCursor struct {
	Timestamp time.Time `json:"timestamp"`
	SpanID    string    `json:"span_id"`
}

type TraceSummary struct {
	TotalTraces int64   `json:"total_traces" ch:"total_traces"`
	ErrorTraces int64   `json:"error_traces" ch:"error_traces"`
	AvgDuration float64 `json:"avg_duration" ch:"avg_duration"`
	P50Duration float64 `json:"p50_duration" ch:"p50_duration"`
	P95Duration float64 `json:"p95_duration" ch:"p95_duration"`
	P99Duration float64 `json:"p99_duration" ch:"p99_duration"`
}

type TraceFacet struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type TraceTrendBucket struct {
	TimeBucket  string  `json:"time_bucket"`
	TotalTraces int64   `json:"total_traces"`
	ErrorTraces int64   `json:"error_traces"`
	P95Duration float64 `json:"p95_duration"`
}

type TraceSearchResponse struct {
	Traces  []Trace      `json:"traces"`
	HasMore bool         `json:"has_more"`
	Offset  int          `json:"offset"`
	Limit   int          `json:"limit"`
	Total   int64        `json:"total"`
	Summary TraceSummary `json:"summary"`
}

type TraceCursorResponse struct {
	Traces     []Trace      `json:"traces"`
	HasMore    bool         `json:"has_more"`
	NextCursor string       `json:"next_cursor,omitempty"`
	Limit      int          `json:"limit"`
	Total      int64        `json:"total,omitempty"`
	Summary    TraceSummary `json:"summary"`
}

type SpanSearchResponse struct {
	Spans      []Trace      `json:"spans"`
	HasMore    bool         `json:"has_more"`
	NextCursor string       `json:"next_cursor,omitempty"`
	Limit      int          `json:"limit"`
	Summary    TraceSummary `json:"summary"`
}

type ServiceDependency struct {
	Source    string `json:"source"     ch:"source"`
	Target    string `json:"target"     ch:"target"`
	CallCount int64  `json:"call_count" ch:"call_count"`
}

type ErrorGroup struct {
	ServiceName     string    `json:"service_name"      ch:"service_name"`
	OperationName   string    `json:"operation_name"    ch:"operation_name"`
	StatusMessage   string    `json:"status_message"    ch:"status_message"`
	HTTPStatusCode  int       `json:"http_status_code"  ch:"http_status_code"`
	ErrorCount      int64     `json:"error_count"       ch:"error_count"`
	LastOccurrence  time.Time `json:"last_occurrence"   ch:"last_occurrence"`
	FirstOccurrence time.Time `json:"first_occurrence"  ch:"first_occurrence"`
	SampleTraceID   string    `json:"sample_trace_id"   ch:"sample_trace_id"`
}

type ErrorTimeSeries struct {
	ServiceName string    `json:"service_name" ch:"service_name"`
	Timestamp   time.Time `json:"timestamp"    ch:"timestamp"`
	TotalCount  int64     `json:"total_count"  ch:"total_count"`
	ErrorCount  int64     `json:"error_count"  ch:"error_count"`
	ErrorRate   float64   `json:"error_rate"   ch:"error_rate"`
}

type LatencyHistogramBucket struct {
	BucketLabel string `json:"bucket_label" ch:"bucket_label"`
	BucketMin   int64  `json:"bucket_min"   ch:"bucket_min"`
	BucketMax   int64  `json:"bucket_max"`
	SpanCount   int64  `json:"span_count"   ch:"span_count"`
}

type LatencyHeatmapPoint struct {
	TimeBucket    time.Time `json:"time_bucket"    ch:"time_bucket"`
	LatencyBucket string    `json:"latency_bucket" ch:"latency_bucket"`
	SpanCount     int64     `json:"span_count"     ch:"span_count"`
}
