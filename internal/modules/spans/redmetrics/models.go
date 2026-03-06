package redmetrics

import "time"

// SlowOperation represents an operation ranked by p99 latency.
type SlowOperation struct {
	OperationName string  `json:"operationName"`
	ServiceName   string  `json:"serviceName"`
	P50Ms         float64 `json:"p50Ms"`
	P95Ms         float64 `json:"p95Ms"`
	P99Ms         float64 `json:"p99Ms"`
	SpanCount     int64   `json:"spanCount"`
}

// ErrorOperation represents an operation ranked by error rate.
type ErrorOperation struct {
	OperationName string  `json:"operationName"`
	ServiceName   string  `json:"serviceName"`
	ExceptionType string  `json:"exceptionType"`
	ErrorRate     float64 `json:"errorRate"`
	ErrorCount    int64   `json:"errorCount"`
	TotalCount    int64   `json:"totalCount"`
}

// HTTPStatusBucket is a count of spans per HTTP status code.
type HTTPStatusBucket struct {
	StatusCode int64 `json:"statusCode"`
	SpanCount  int64 `json:"spanCount"`
}

// HTTPStatusTimePoint is a single time-series point per HTTP status code.
type HTTPStatusTimePoint struct {
	Timestamp  time.Time `json:"timestamp"`
	StatusCode int64     `json:"statusCode"`
	SpanCount  int64     `json:"spanCount"`
}

// ServiceScorecard is a per-service RED summary tile.
type ServiceScorecard struct {
	ServiceName string  `json:"serviceName"`
	RPS         float64 `json:"rps"`
	ErrorPct    float64 `json:"errorPct"`
	P95Ms       float64 `json:"p95Ms"`
}

// ApdexScore is a per-service Apdex satisfaction score.
type ApdexScore struct {
	ServiceName  string  `json:"serviceName"`
	Apdex        float64 `json:"apdex"`
	Satisfied    int64   `json:"satisfied"`
	Tolerating   int64   `json:"tolerating"`
	Frustrated   int64   `json:"frustrated"`
	TotalCount   int64   `json:"totalCount"`
}
