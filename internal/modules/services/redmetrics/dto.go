package redmetrics

type redSummaryServiceRow struct {
	ServiceName string  `ch:"service"`
	TotalCount  uint64  `ch:"total_count"`
	ErrorCount  uint64  `ch:"error_count"`
	P50Ms       float32 `ch:"p50_ms"`
	P95Ms       float32 `ch:"p95_ms"`
	P99Ms       float32 `ch:"p99_ms"`
}

type apdexRow struct {
	ServiceName string `ch:"service"`
	TotalCount  uint64 `ch:"total_count"`
	Satisfied   uint64 `ch:"satisfied"`
	Tolerating  uint64 `ch:"tolerating"`
}

type latencyBreakdownRow struct {
	ServiceName string  `ch:"service"`
	TotalMs     float64 `ch:"total_ms"`
	SpanCount   uint64  `ch:"span_count"`
}

type slowOperationRow struct {
	ServiceName   string  `ch:"service"`
	OperationName string  `ch:"operation_name"`
	SpanCount     uint64  `ch:"span_count"`
	P50Ms         float32 `ch:"p50_ms"`
	P95Ms         float32 `ch:"p95_ms"`
	P99Ms         float32 `ch:"p99_ms"`
}

type errorOperationRow struct {
	ServiceName   string `ch:"service"`
	OperationName string `ch:"operation_name"`
	TotalCount    uint64 `ch:"total_count"`
	ErrorCount    uint64 `ch:"error_count"`
}
