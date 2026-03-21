package traces

import "time"

// traceRow is the scan target for GetTracesKeyset / GetTraces.
// EndTime is computed by the service (StartTime + DurationNano).
type traceRow struct {
	SpanID         string    `ch:"span_id"`
	TraceID        string    `ch:"trace_id"`
	ServiceName    string    `ch:"service_name"`
	OperationName  string    `ch:"operation_name"`
	StartTime      time.Time `ch:"start_time"`
	DurationNano   int64     `ch:"duration_nano"`
	DurationMs     float64   `ch:"duration_ms"`
	Status         string    `ch:"status"`
	StatusMessage  string    `ch:"status_message"`
	HTTPMethod     string    `ch:"http_method"`
	HTTPStatusCode uint16    `ch:"http_status_code"`
	ParentSpanID   string    `ch:"parent_span_id"`
	SpanKind       string    `ch:"span_kind"`
}

type traceSummaryRow struct {
	TotalTraces int64   `ch:"total_traces"`
	ErrorTraces int64   `ch:"error_traces"`
	AvgDuration float64 `ch:"avg_duration"`
	P50Duration float64 `ch:"p50_duration"`
	P95Duration float64 `ch:"p95_duration"`
	P99Duration float64 `ch:"p99_duration"`
}

type traceCountRow struct {
	Total int64 `ch:"total"`
}

type traceFacetRow struct {
	Key   string `ch:"facet_key"`
	Value string `ch:"facet_value"`
	Count int64  `ch:"count"`
}

type traceTrendRow struct {
	TimeBucket  string  `ch:"time_bucket"`
	TotalTraces int64   `ch:"total_traces"`
	ErrorTraces int64   `ch:"error_traces"`
	P95Duration float64 `ch:"p95_duration"`
}

// spanRow is the scan target for GetTraceSpans / GetSpanTree.
// EndTime is computed by the service (StartTime + DurationNano).
type spanRow struct {
	SpanID         string    `ch:"span_id"`
	ParentSpanID   string    `ch:"parent_span_id"`
	TraceID        string    `ch:"trace_id"`
	OperationName  string    `ch:"operation_name"`
	ServiceName    string    `ch:"service_name"`
	SpanKind       string    `ch:"span_kind"`
	StartTime      time.Time `ch:"start_time"`
	DurationNano   int64     `ch:"duration_nano"`
	DurationMs     float64   `ch:"duration_ms"`
	Status         string    `ch:"status"`
	StatusMessage  string    `ch:"status_message"`
	HTTPMethod     string    `ch:"http_method"`
	HTTPURL        string    `ch:"http_url"`
	HTTPStatusCode uint16    `ch:"http_status_code"`
	Host           string    `ch:"host"`
	Pod            string    `ch:"pod"`
	Attributes     string    `ch:"attributes"`
}

type serviceDependencyRow struct {
	Source    string `ch:"source"`
	Target    string `ch:"target"`
	CallCount int64  `ch:"call_count"`
}

type errorGroupRow struct {
	ServiceName     string    `ch:"service_name"`
	OperationName   string    `ch:"operation_name"`
	StatusMessage   string    `ch:"status_message"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      int64     `ch:"error_count"`
	LastOccurrence  time.Time `ch:"last_occurrence"`
	FirstOccurrence time.Time `ch:"first_occurrence"`
	SampleTraceID   string    `ch:"sample_trace_id"`
}

type errorTimeSeriesRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	TotalCount  int64     `ch:"total_count"`
	ErrorCount  int64     `ch:"error_count"`
	ErrorRate   float64   `ch:"error_rate"`
}

// latencyHistogramRow is the scan target for GetLatencyHistogram.
// BucketMax is set by the service (BucketMin + 1).
type latencyHistogramRow struct {
	BucketLabel string `ch:"bucket_label"`
	BucketMin   int64  `ch:"bucket_min"`
	SpanCount   int64  `ch:"span_count"`
}

type latencyHeatmapRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	LatencyBucket string    `ch:"latency_bucket"`
	SpanCount     int64     `ch:"span_count"`
}
