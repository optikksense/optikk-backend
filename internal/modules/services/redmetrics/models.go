package redmetrics

import "time"

type REDSummary struct {
	ServiceCount   int64              `json:"service_count"`
	TotalSpanCount int64              `json:"total_span_count"`
	TotalErrors    int64              `json:"total_errors"`
	TotalRPS       float64            `json:"total_rps"`
	AvgErrorPct    float64            `json:"avg_error_pct"`
	AvgP50Ms       float64            `json:"avg_p50_ms"`
	AvgP95Ms       float64            `json:"avg_p95_ms"`
	AvgP99Ms       float64            `json:"avg_p99_ms"`
	Services       []ServiceREDMetric `json:"services"`
}

type ServiceREDMetric struct {
	ServiceName       string  `json:"service_name"`
	RequestCount      int64   `json:"request_count"`
	ErrorCount        int64   `json:"error_count"`
	AvgLatency        float64 `json:"avg_latency"`
	P95Latency        float64 `json:"p95_latency"`
	P99Latency        float64 `json:"p99_latency"`
	CPUUtilization    float64 `json:"cpu_utilization"`
	MemoryUtilization float64 `json:"memory_utilization"`
	DiskUtilization   float64 `json:"disk_utilization"`
}


type ApdexScore struct {
	ServiceName string  `json:"service_name"`
	Apdex       float64 `json:"apdex"`
	Satisfied   int64   `json:"satisfied"`
	Tolerating  int64   `json:"tolerating"`
	Frustrated  int64   `json:"frustrated"`
	TotalCount  int64   `json:"total_count"`
}

type SlowOperation struct {
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ServiceName   string  `json:"service_name"   ch:"service"`
	P50Ms         float64 `json:"p50_ms"         ch:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"         ch:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"         ch:"p99_ms"`
	SpanCount     int64   `json:"span_count"     ch:"span_count"`
}

type ErrorOperation struct {
	OperationName string  `json:"operation_name" ch:"operation_name"`
	ServiceName   string  `json:"service_name"   ch:"service"`
	ExceptionType string  `json:"exception_type" ch:"exception_type"`
	ErrorRate     float64 `json:"error_rate"     ch:"error_rate"`
	ErrorCount    int64   `json:"error_count"    ch:"error_count"`
	TotalCount    int64   `json:"total_count"    ch:"total_count"`
}

type ServiceRatePoint struct {
	Timestamp   time.Time `json:"timestamp"    ch:"timestamp"`
	ServiceName string    `json:"service_name" ch:"service"`
	RPS         float64   `json:"rps"          ch:"rps"`
}

type ServiceLatencyPoint struct {
	Timestamp   time.Time `json:"timestamp"    ch:"timestamp"`
	ServiceName string    `json:"service_name" ch:"service"`
	P95Ms       float64   `json:"p95_ms"       ch:"p95_ms"`
}

type SpanKindPoint struct {
	Timestamp  time.Time `json:"timestamp"   ch:"timestamp"`
	KindString string    `json:"kind_string" ch:"kind_string"`
	SpanCount  int64     `json:"span_count"  ch:"span_count"`
}

type ErrorByRoutePoint struct {
	Timestamp    time.Time `json:"timestamp"     ch:"timestamp"`
	HttpRoute    string    `json:"http_route"    ch:"http_route"`
	RequestCount int64     `json:"request_count" ch:"request_count"`
	ErrorCount   int64     `json:"error_count"   ch:"error_count"`
}

// LatencyBreakdown shows aggregate time spent per service.
type LatencyBreakdown struct {
	ServiceName string  `json:"serviceName"`
	TotalMs     float64 `json:"totalMs"`
	SpanCount   int64   `json:"spanCount"`
	PctOfTotal  float64 `json:"pctOfTotal"`
}

// StatusTimeSeriesPoint is one display-bucket row with span counts split by
// the OTel http.status_code bucket (`2xx` / `4xx` / `5xx`).
type StatusTimeSeriesPoint struct {
	Timestamp   time.Time `json:"timestamp"`
	Status2xx   float64   `json:"status_2xx"`
	Status4xx   float64   `json:"status_4xx"`
	Status5xx   float64   `json:"status_5xx"`
	StatusOther float64   `json:"status_other"`
}

// LatencyPercentilesPoint is one display-bucket row with p50/p95/p99 latency.
type LatencyPercentilesPoint struct {
	Timestamp time.Time `json:"timestamp"`
	P50Ms     float64   `json:"p50_ms"`
	P95Ms     float64   `json:"p95_ms"`
	P99Ms     float64   `json:"p99_ms"`
}

// TopEndpoint is one per-operation row used by the Service Detail endpoints
// table — combines rate, error %, and p50/p95/p99 latency.
type TopEndpoint struct {
	OperationName string  `json:"operation_name"`
	ServiceName   string  `json:"service_name"`
	SpanKind      string  `json:"span_kind"`
	HTTPRoute     string  `json:"http_route"`
	RPS           float64 `json:"rps"`
	ErrorRate     float64 `json:"error_rate"`
	ErrorCount    int64   `json:"error_count"`
	TotalCount    int64   `json:"total_count"`
	P50Ms         float64 `json:"p50_ms"`
	P95Ms         float64 `json:"p95_ms"`
	P99Ms         float64 `json:"p99_ms"`
}

type ServiceSummaryResponse struct {
	ServiceName       string  `json:"service_name"`
	RequestCount      int64   `json:"request_count"`
	ErrorCount        int64   `json:"error_count"`
	RPS               float64 `json:"rps"`
	ErrorRate         float64 `json:"error_rate"`
	P50Ms             float64 `json:"p50_ms"`
	P95Ms             float64 `json:"p95_ms"`
	P99Ms             float64 `json:"p99_ms"`
	CPUUtilization    float64 `json:"cpu_utilization"`
	MemoryUtilization float64 `json:"memory_utilization"`
	DiskUtilization   float64 `json:"disk_utilization"`
}

type SaturationTimeSeriesPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type serviceMetricTimeseriesRow struct {
	BucketAt   time.Time `ch:"bucket_at"`
	MetricName string    `ch:"metric_name"`
	Value      float64   `ch:"value"`
}

type serviceMetricRow struct {
	Service    string  `ch:"service"`
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

