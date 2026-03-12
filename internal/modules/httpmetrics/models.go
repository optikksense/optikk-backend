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
