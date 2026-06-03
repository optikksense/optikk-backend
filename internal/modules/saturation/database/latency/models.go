package latency

type LatencyTimeSeries struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy    string   `json:"group_by" ch:"group_by"`
	P50Ms      *float64 `json:"p50_ms" ch:"p50_ms"`
	P95Ms      *float64 `json:"p95_ms" ch:"p95_ms"`
	P99Ms      *float64 `json:"p99_ms" ch:"p99_ms"`
}
