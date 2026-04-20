package latency

type LatencyTimeSeries struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy    string   `json:"group_by" ch:"group_by"`
	P50Ms      *float64 `json:"p50_ms" ch:"p50_ms"`
	P95Ms      *float64 `json:"p95_ms" ch:"p95_ms"`
	P99Ms      *float64 `json:"p99_ms" ch:"p99_ms"`

	// Raw sum/count are used by the service for avg computations on endpoints
	// that surface avg. Not serialized.
	LatencySum   float64 `json:"-" ch:"-"`
	LatencyCount int64   `json:"-" ch:"-"`
}

// LatencyHeatmapBucket is the public per-(time_bucket, bucket_label) output
// returned from the service. The service computes `bucket_label` + `density`
// from a stream of raw LatencyHeatmapSample values in Go so the SQL stays
// free of banned conditional / null-fallback combinators.
type LatencyHeatmapBucket struct {
	TimeBucket  string  `json:"time_bucket"`
	BucketLabel string  `json:"bucket_label"`
	Count       int64   `json:"count"`
	Density     float64 `json:"density"`
}

// LatencyHeatmapSample is one raw (time_bucket, avg_sec, count) datapoint
// returned by the repository. The service layer classifies each sample into
// a labelled latency bucket.
type LatencyHeatmapSample struct {
	TimeBucket string
	AvgSec     float64
	Count      int64
}

// latencyHeatmapSampleDTO is the CH scan target for the heatmap query. CH
// returns count() as UInt64 natively, so we scan into uint64 and convert.
type latencyHeatmapSampleDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	AvgSec     float64 `ch:"avg_sec"`
	Count      uint64  `ch:"count"`
}
