package topology

// nodeAggRow is scanned from the per-service RED aggregation query. The
// Buckets array holds non-cumulative counts aligned with latencyBucketBoundsMs
// (defined in service.go); percentiles are interpolated Go-side via
// quantile.FromHistogram.
type nodeAggRow struct {
	ServiceName  string   `ch:"service"`
	RequestCount int64    `ch:"request_count"`
	ErrorCount   int64    `ch:"error_count"`
	Buckets      []uint64 `ch:"bucket_counts"`
}

// edgeAggRow is scanned from the per-(service → peer_service) edge
// aggregation. Same Buckets shape as nodeAggRow.
type edgeAggRow struct {
	Source     string   `ch:"source"`
	Target     string   `ch:"target"`
	CallCount  int64    `ch:"call_count"`
	ErrorCount int64    `ch:"error_count"`
	Buckets    []uint64 `ch:"bucket_counts"`
}
