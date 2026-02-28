package traces

import "time"

// Trace represents a root span (trace summary).
type Trace struct {
	SpanID         string    `json:"spanId"`
	TraceID        string    `json:"traceId"`
	ServiceName    string    `json:"serviceName"`
	OperationName  string    `json:"operationName"`
	StartTime      time.Time `json:"startTime"`
	EndTime        time.Time `json:"endTime"`
	DurationMs     float64   `json:"durationMs"`
	Status         string    `json:"status"`
	HTTPMethod     string    `json:"httpMethod"`
	HTTPStatusCode int       `json:"httpStatusCode"`
}

// Span represents a single span in a trace.
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

// TraceFilters defines the search criteria for traces.
type TraceFilters struct {
	TeamUUID    string   `json:"teamUuid"`
	StartMs     int64    `json:"startMs"`
	EndMs       int64    `json:"endMs"`
	Services    []string `json:"services"`
	Status      string   `json:"status"`
	MinDuration string   `json:"minDuration"`
	MaxDuration string   `json:"maxDuration"`
	TraceID     string   `json:"traceId"`
	Operation   string   `json:"operation"`
	HTTPStatus  string   `json:"httpStatus"`
}

// TraceSummary represents aggregate statistics for a set of traces.
type TraceSummary struct {
	TotalTraces int64   `json:"totalTraces"`
	ErrorTraces int64   `json:"errorTraces"`
	AvgDuration float64 `json:"avgDuration"`
	P50Duration float64 `json:"p50Duration"`
	P95Duration float64 `json:"p95Duration"`
	P99Duration float64 `json:"p99Duration"`
}

// TraceSearchResponse represents the result of a trace search.
type TraceSearchResponse struct {
	Traces  []Trace      `json:"traces"`
	HasMore bool         `json:"hasMore"`
	Offset  int          `json:"offset"`
	Limit   int          `json:"limit"`
	Total   int64        `json:"total"`
	Summary TraceSummary `json:"summary"`
}

// ServiceDependency represents a call from one service to another.
type ServiceDependency struct {
	Source    string `json:"source"`
	Target    string `json:"target"`
	CallCount int64  `json:"callCount"`
}

// ErrorGroup represents an aggregation of similar errors.
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

// ErrorTimeSeries represents the error rate over time for a service.
type ErrorTimeSeries struct {
	ServiceName string    `json:"serviceName"`
	Timestamp   time.Time `json:"timestamp"`
	TotalCount  int64     `json:"totalCount"`
	ErrorCount  int64     `json:"errorCount"`
	ErrorRate   float64   `json:"errorRate"`
}

// LatencyHistogramBucket represents a count of spans within a duration range.
type LatencyHistogramBucket struct {
	BucketLabel string `json:"bucketLabel"`
	BucketMin   int64  `json:"bucketMin"`
	BucketMax   int64  `json:"bucketMax"`
	SpanCount   int64  `json:"spanCount"`
}

// LatencyHeatmapPoint represents a counts of spans for a time bucket and latency bucket.
type LatencyHeatmapPoint struct {
	TimeBucket    time.Time `json:"timeBucket"`
	LatencyBucket string    `json:"latencyBucket"`
	SpanCount     int64     `json:"spanCount"`
}
