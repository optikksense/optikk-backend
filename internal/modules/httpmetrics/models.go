package httpmetrics

// StatusCodeBucket represents request counts grouped by status code over time.
type StatusCodeBucket struct {
	Timestamp  string `json:"timestamp"`
	StatusCode string `json:"status_code"`
	Count      int64  `json:"count"`
}

// TimeBucket represents a simple scalar timeseries point.
type TimeBucket struct {
	Timestamp string   `json:"timestamp"`
	Value     *float64 `json:"value"`
}

// HistogramSummary holds p50/p95/p99/avg for histogram metrics.
type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}
