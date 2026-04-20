package query

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

// traceSummaryRow is the merged result of the totals + errors summary legs.
// Percentile fields are zero-filled and overwritten from sketch.Querier.
// AvgDuration is computed Go-side from DurationMsSum / DurationMsCount.
type traceSummaryRow struct {
	TotalTraces     uint64
	ErrorTraces     uint64
	DurationMsSum   float64
	DurationMsCount uint64
	P50Duration     float64
	P95Duration     float64
	P99Duration     float64
}

// traceSummaryTotalsRow is the scan target for the totals summary leg.
type traceSummaryTotalsRow struct {
	TotalTraces   uint64  `ch:"total_traces"`
	DurationMsSum float64 `ch:"duration_ms_sum"`
}

// traceCountRow is a single-value count scan target used by both the errors
// summary leg and paging totals.
type traceCountRow struct {
	Total uint64 `ch:"total"`
}

// spanTraceIDRow is the scan target for the GetSpanTree lookup.
type spanTraceIDRow struct {
	TraceID string `ch:"trace_id"`
}

type traceFacetRow struct {
	Key   string `ch:"facet_key"`
	Value string `ch:"facet_value"`
	Count uint64 `ch:"count"`
}

// traceTrendRow is the merged (totals + errors) trend bucket. P95Duration
// is filled from sketch.Querier in the service layer.
type traceTrendRow struct {
	TimeBucket  string
	TotalTraces uint64
	ErrorTraces uint64
	P95Duration float64
}

// traceTrendTotalsRow is the scan target for the totals trend leg.
type traceTrendTotalsRow struct {
	TimeBucket  string `ch:"time_bucket"`
	TotalTraces uint64 `ch:"total_traces"`
}

// traceTrendErrorRow is the scan target for the error trend leg.
type traceTrendErrorRow struct {
	TimeBucket  string `ch:"time_bucket"`
	ErrorTraces uint64 `ch:"error_traces"`
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

type errorGroupRow struct {
	ServiceName     string    `ch:"service_name"`
	OperationName   string    `ch:"operation_name"`
	StatusMessage   string    `ch:"status_message"`
	HTTPStatusCode  uint16    `ch:"http_status_code"`
	ErrorCount      uint64    `ch:"error_count"`
	LastOccurrence  time.Time `ch:"last_occurrence"`
	FirstOccurrence time.Time `ch:"first_occurrence"`
	SampleTraceID   string    `ch:"sample_trace_id"`
}

// errorTimeSeriesRow is the merged (totals + errors) per-(bucket, service)
// error-rate row. ErrorRate is computed Go-side from the two legs.
type errorTimeSeriesRow struct {
	ServiceName string    `json:"service_name" ch:"service_name"`
	Timestamp   time.Time `json:"timestamp"    ch:"timestamp"`
	TotalCount  uint64    `json:"total_count"  ch:"total_count"`
	ErrorCount  uint64    `json:"error_count"  ch:"error_count"`
	ErrorRate   float64   `json:"error_rate"   ch:"error_rate"`
}

// errorTimeSeriesTotalsRow is the scan target for the totals leg.
type errorTimeSeriesTotalsRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	TotalCount  uint64    `ch:"total_count"`
}

// errorTimeSeriesErrorRow is the scan target for the error-only leg.
type errorTimeSeriesErrorRow struct {
	ServiceName string    `ch:"service_name"`
	Timestamp   time.Time `ch:"timestamp"`
	ErrorCount  uint64    `ch:"error_count"`
}

// latencyHistogramRow is the scan target for GetLatencyHistogram. The
// bucket label string is formatted Go-side from BucketMin so no string
// cast is needed in SQL.
type latencyHistogramRow struct {
	BucketMin float64 `ch:"bucket_min"`
	SpanCount uint64  `ch:"span_count"`
}

// latencyHeatmapRow carries the numeric latency_bucket (log10 * 10 / 10).
// The service formats it to the old string label so SQL stays combinator-free.
type latencyHeatmapRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	LatencyBucket float64   `ch:"latency_bucket"`
	SpanCount     uint64    `ch:"span_count"`
}
