package latency

type histogramRow struct {
	P50 float32 `ch:"p50"`
	P90 float32 `ch:"p90"`
	P95 float32 `ch:"p95"`
	P99 float32 `ch:"p99"`
	Max float64 `ch:"max"`
	Avg float64 `ch:"avg"`
}

type heatmapRow struct {
	TsBucket uint32  `ch:"ts_bucket"`
	BucketMs float64 `ch:"bucket_ms"`
	Count    uint64  `ch:"count"`
}
