package topology

// nodeAggRow is scanned from the per-service RED aggregation query.
type nodeAggRow struct {
	ServiceName  string    `ch:"service"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
	QS           []float32 `ch:"qs"`
	P50Ms        float32   `ch:"p50_ms"`
	P95Ms        float32   `ch:"p95_ms"`
	P99Ms        float32   `ch:"p99_ms"`
}

// edgeAggRow is scanned from the service-to-peer edge aggregation query.
type edgeAggRow struct {
	Source     string    `ch:"source"`
	Target     string    `ch:"target"`
	CallCount  uint64    `ch:"call_count"`
	ErrorCount uint64    `ch:"error_count"`
	QS         []float32 `ch:"qs"`
	P50Ms      float32   `ch:"p50_ms"`
	P95Ms      float32   `ch:"p95_ms"`
}
