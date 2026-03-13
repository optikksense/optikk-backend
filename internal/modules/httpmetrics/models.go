package httpmetrics

type StatusCodeBucket struct {
	Timestamp  string `json:"timestamp"`
	StatusCode string `json:"status_code"`
	Count      int64  `json:"count"`
}

type TimeBucket struct {
	Timestamp string   `json:"timestamp"`
	Value     *float64 `json:"value"`
}

type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}

type RouteMetric struct {
	Route    string  `json:"route"`
	ReqCount int64   `json:"req_count"`
	P95Ms    float64 `json:"p95_ms,omitempty"`
	ErrorPct float64 `json:"error_pct,omitempty"`
}

type RouteTimeseriesPoint struct {
	Timestamp  string `json:"timestamp"`
	HttpRoute  string `json:"http_route"`
	ErrorCount int64  `json:"error_count"`
}

type StatusGroupBucket struct {
	StatusGroup string `json:"status_group"`
	Count       int64  `json:"count"`
}

type ErrorTimeseriesPoint struct {
	Timestamp  string  `json:"timestamp"`
	ReqCount   int64   `json:"req_count"`
	ErrorCount int64   `json:"error_count"`
	ErrorRate  float64 `json:"error_rate"`
}

type ExternalHostMetric struct {
	Host     string  `json:"host"`
	ReqCount int64   `json:"req_count,omitempty"`
	P95Ms    float64 `json:"p95_ms,omitempty"`
	ErrorPct float64 `json:"error_pct,omitempty"`
}
