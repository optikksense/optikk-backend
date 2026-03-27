package redmetrics

import "time"

type REDSummary struct {
	ServiceCount int64   `json:"service_count"`
	TotalRPS     float64 `json:"total_rps"`
	AvgErrorPct  float64 `json:"avg_error_pct"`
	AvgP95Ms     float64 `json:"avg_p95_ms"`
}

type ServiceScorecard struct {
	ServiceName string  `json:"service_name"`
	RPS         float64 `json:"rps"`
	ErrorPct    float64 `json:"error_pct"`
	P95Ms       float64 `json:"p95_ms"`
}

type ApdexScore struct {
	ServiceName string  `json:"service_name"`
	Apdex       float64 `json:"apdex"`
	Satisfied   int64   `json:"satisfied"`
	Tolerating  int64   `json:"tolerating"`
	Frustrated  int64   `json:"frustrated"`
	TotalCount  int64   `json:"total_count"`
}

type HTTPStatusBucket struct {
	StatusCode int64 `json:"status_code" ch:"status_code"`
	SpanCount  int64 `json:"span_count"  ch:"span_count"`
}

type SlowOperation struct {
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ServiceName   string  `json:"service_name"   ch:"service_name"`
	P50Ms         float64 `json:"p50_ms"         ch:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"         ch:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"         ch:"p99_ms"`
	SpanCount     int64   `json:"span_count"     ch:"span_count"`
}

type ErrorOperation struct {
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ServiceName   string  `json:"service_name"   ch:"service_name"`
	ExceptionType string  `json:"exception_type" ch:"exception_type"`
	ErrorRate     float64 `json:"error_rate"     ch:"error_rate"`
	ErrorCount    int64   `json:"error_count"    ch:"error_count"`
	TotalCount    int64   `json:"total_count"    ch:"total_count"`
}

type ServiceRatePoint struct {
	Timestamp   time.Time `json:"timestamp"    ch:"timestamp"`
	ServiceName string    `json:"service_name" ch:"service_name"`
	RPS         float64   `json:"rps"          ch:"rps"`
}

type ServiceErrorRatePoint struct {
	Timestamp   time.Time `json:"timestamp"    ch:"timestamp"`
	ServiceName string    `json:"service_name" ch:"service_name"`
	ErrorPct    float64   `json:"error_pct"    ch:"error_pct"`
}

type ServiceLatencyPoint struct {
	Timestamp   time.Time `json:"timestamp"    ch:"timestamp"`
	ServiceName string    `json:"service_name" ch:"service_name"`
	P95Ms       float64   `json:"p95_ms"       ch:"p95_ms"`
}

type SpanKindPoint struct {
	Timestamp  time.Time `json:"timestamp"   ch:"timestamp"`
	KindString string    `json:"kind_string" ch:"kind_string"`
	SpanCount  int64     `json:"span_count"  ch:"span_count"`
}

type ErrorByRoutePoint struct {
	Timestamp  time.Time `json:"timestamp"   ch:"timestamp"`
	HttpRoute  string    `json:"http_route"  ch:"http_route"`
	ErrorCount int64     `json:"error_count" ch:"error_count"`
}

// LatencyBreakdown shows aggregate time spent per service.
type LatencyBreakdown struct {
	ServiceName string  `json:"serviceName"`
	TotalMs     float64 `json:"totalMs"`
	SpanCount   int64   `json:"spanCount"`
	PctOfTotal  float64 `json:"pctOfTotal"`
}
