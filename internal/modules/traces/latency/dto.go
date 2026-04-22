package latency

type HistogramRequest struct {
	StartMs     int64  `form:"startTime" binding:"required"`
	EndMs       int64  `form:"endTime" binding:"required"`
	ServiceName string `form:"service"`
	Operation   string `form:"operation"`
}

type HistogramResponse struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
	Avg float64 `json:"avg"`
}

type HeatmapRequest struct {
	StartMs     int64  `form:"startTime" binding:"required"`
	EndMs       int64  `form:"endTime" binding:"required"`
	ServiceName string `form:"service"`
}

type HeatmapCell struct {
	TimeBucket string  `json:"time_bucket"`
	BucketMs   float64 `json:"bucket_ms"`
	Count      uint64  `json:"count"`
}

type HeatmapResponse struct {
	Cells []HeatmapCell `json:"cells"`
}
