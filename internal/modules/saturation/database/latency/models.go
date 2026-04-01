package latency

type LatencyTimeSeries struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy    string   `json:"group_by" ch:"group_by"`
	P50Ms      *float64 `json:"p50_ms" ch:"p50_ms"`
	P95Ms      *float64 `json:"p95_ms" ch:"p95_ms"`
	P99Ms      *float64 `json:"p99_ms" ch:"p99_ms"`
}

type LatencyHeatmapBucket struct {
	TimeBucket  string  `json:"time_bucket"`
	BucketLabel string  `json:"bucket_label"`
	Count       int64   `json:"count"`
	Density     float64 `json:"density"`
}

type latencyHeatmapDTO struct {
	TimeBucket  string `ch:"time_bucket"`
	BucketLabel string `ch:"bucket_label"`
	Count       int64  `ch:"count"`
}
