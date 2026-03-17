package httpmetrics

type StatusCodeBucket struct {
	Timestamp  string `json:"timestamp"   ch:"time_bucket"`
	StatusCode string `json:"status_code" ch:"status_code"`
	Count      int64  `json:"count"       ch:"req_count"`
}

type TimeBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Value     *float64 `json:"value"     ch:"val"`
}

type HistogramSummary struct {
	P50 float64 `json:"p50" ch:"p50"`
	P95 float64 `json:"p95" ch:"p95"`
	P99 float64 `json:"p99" ch:"p99"`
	Avg float64 `json:"avg" ch:"avg"`
}

type RouteMetric struct {
	Route    string  `json:"route"              ch:"route"`
	ReqCount int64   `json:"req_count"          ch:"req_count"`
	P95Ms    float64 `json:"p95_ms,omitempty"   ch:"p95_ms"`
	ErrorPct float64 `json:"error_pct,omitempty" ch:"error_pct"`
}

type RouteTimeseriesPoint struct {
	Timestamp  string `json:"timestamp"   ch:"timestamp"`
	HttpRoute  string `json:"http_route"  ch:"http_route"`
	ErrorCount int64  `json:"error_count" ch:"error_count"`
}

type StatusGroupBucket struct {
	StatusGroup string `json:"status_group" ch:"status_group"`
	Count       int64  `json:"count"        ch:"count"`
}

type ErrorTimeseriesPoint struct {
	Timestamp  string  `json:"timestamp"   ch:"timestamp"`
	ReqCount   int64   `json:"req_count"   ch:"req_count"`
	ErrorCount int64   `json:"error_count" ch:"error_count"`
	ErrorRate  float64 `json:"error_rate"  ch:"error_rate"`
}

type ExternalHostMetric struct {
	Host     string  `json:"host"               ch:"host"`
	ReqCount int64   `json:"req_count,omitempty" ch:"req_count"`
	P95Ms    float64 `json:"p95_ms,omitempty"   ch:"p95_ms"`
	ErrorPct float64 `json:"error_pct,omitempty" ch:"error_pct"`
}
