package topology

// nodeAggRow is scanned from the per-service RED aggregation query.
type nodeAggRow struct {
	ServiceName  string  `ch:"service"`
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	P50Ms        float64 `ch:"p50_ms"`
	P95Ms        float64 `ch:"p95_ms"`
	P99Ms        float64 `ch:"p99_ms"`
}

// edgeAggRow is scanned from the parent→child span join query.
type edgeAggRow struct {
	Source     string  `ch:"source"`
	Target     string  `ch:"target"`
	CallCount  int64   `ch:"call_count"`
	ErrorCount int64   `ch:"error_count"`
	P50Ms      float64 `ch:"p50_ms"`
	P95Ms      float64 `ch:"p95_ms"`
}
