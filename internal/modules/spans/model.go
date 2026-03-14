package traces

import "time"

type Trace struct {
	SpanID         string    `json:"spanId"`
	TraceID        string    `json:"traceId"`
	ServiceName    string    `json:"serviceName"`
	OperationName  string    `json:"operationName"`
	StartTime      time.Time `json:"startTime"`
	EndTime        time.Time `json:"endTime"`
	DurationMs     float64   `json:"durationMs"`
	Status         string    `json:"status"`
	StatusMessage  string    `json:"statusMessage"`
	HTTPMethod     string    `json:"httpMethod"`
	HTTPStatusCode int       `json:"httpStatusCode"`
	// ParentSpanID and SpanKind are populated only in span-level search (SearchMode="all").
	ParentSpanID string `json:"parentSpanId,omitempty"`
	SpanKind     string `json:"spanKind,omitempty"`
}

type Span struct {
	SpanID         string    `json:"spanId"`
	ParentSpanID   string    `json:"parentSpanId"`
	TraceID        string    `json:"traceId"`
	OperationName  string    `json:"operationName"`
	ServiceName    string    `json:"serviceName"`
	SpanKind       string    `json:"spanKind"`
	StartTime      time.Time `json:"startTime"`
	EndTime        time.Time `json:"endTime"`
	DurationMs     float64   `json:"durationMs"`
	Status         string    `json:"status"`
	StatusMessage  string    `json:"statusMessage"`
	HTTPMethod     string    `json:"httpMethod"`
	HTTPURL        string    `json:"httpUrl"`
	HTTPStatusCode int       `json:"httpStatusCode"`
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
	SpanID    string    `json:"spanId"`
}

type TraceOperationRow struct {
	ServiceName   string  `json:"serviceName"`
	OperationName string  `json:"operationName"`
	SpanCount     int64   `json:"spanCount"`
	ErrorCount    int64   `json:"errorCount"`
	ErrorRate     float64 `json:"errorRate"`
	P50Ms         float64 `json:"p50Ms"`
	P95Ms         float64 `json:"p95Ms"`
	P99Ms         float64 `json:"p99Ms"`
	AvgMs         float64 `json:"avgMs"`
}

type TraceSummary struct {
	TotalTraces int64   `json:"totalTraces"`
	ErrorTraces int64   `json:"errorTraces"`
	AvgDuration float64 `json:"avgDuration"`
	P50Duration float64 `json:"p50Duration"`
	P95Duration float64 `json:"p95Duration"`
	P99Duration float64 `json:"p99Duration"`
}

type TraceSearchResponse struct {
	Traces  []Trace      `json:"traces"`
	HasMore bool         `json:"hasMore"`
	Offset  int          `json:"offset"`
	Limit   int          `json:"limit"`
	Total   int64        `json:"total"`
	Summary TraceSummary `json:"summary"`
}

type ServiceDependency struct {
	Source    string `json:"source"`
	Target    string `json:"target"`
	CallCount int64  `json:"callCount"`
}

type ErrorGroup struct {
	ServiceName     string    `json:"serviceName"`
	OperationName   string    `json:"operationName"`
	StatusMessage   string    `json:"statusMessage"`
	HTTPStatusCode  int       `json:"httpStatusCode"`
	ErrorCount      int64     `json:"errorCount"`
	LastOccurrence  time.Time `json:"lastOccurrence"`
	FirstOccurrence time.Time `json:"firstOccurrence"`
	SampleTraceID   string    `json:"sampleTraceId"`
}

type ErrorTimeSeries struct {
	ServiceName string    `json:"serviceName"`
	Timestamp   time.Time `json:"timestamp"`
	TotalCount  int64     `json:"totalCount"`
	ErrorCount  int64     `json:"errorCount"`
	ErrorRate   float64   `json:"errorRate"`
}

type LatencyHistogramBucket struct {
	BucketLabel string `json:"bucketLabel"`
	BucketMin   int64  `json:"bucketMin"`
	BucketMax   int64  `json:"bucketMax"`
	SpanCount   int64  `json:"spanCount"`
}

type LatencyHeatmapPoint struct {
	TimeBucket    time.Time `json:"timeBucket"`
	LatencyBucket string    `json:"latencyBucket"`
	SpanCount     int64     `json:"spanCount"`
}
