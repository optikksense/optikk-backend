package redmetrics

import "time"

type redSummaryServiceRow struct {
	ServiceName string  `ch:"service_name"`
	TotalCount  int64   `ch:"total_count"`
	ErrorCount  int64   `ch:"error_count"`
	// Percentiles are filled by the service layer from sketch.Querier.
	P50Ms float64 `ch:"p50_ms"`
	P95Ms float64 `ch:"p95_ms"`
	P99Ms float64 `ch:"p99_ms"`
}

type apdexRow struct {
	ServiceName string `ch:"service_name"`
	Satisfied   int64  `ch:"satisfied"`
	Tolerating  int64  `ch:"tolerating"`
	Frustrated  int64  `ch:"frustrated"`
	TotalCount  int64  `ch:"total_count"`
}

// latencyBreakdownRow scans the CH aggregation; the mean is computed in
// service.go from sum + count (SQL emits raw sum only).
type latencyBreakdownRow struct {
	ServiceName string  `ch:"service_name"`
	TotalMsSum  float64 `ch:"total_ms_sum"`
	SpanCount   int64   `ch:"span_count"`
}

type slowOperationRow struct {
	ServiceName   string  `ch:"service_name"`
	OperationName string  `ch:"operation_name"`
	SpanCount     int64   `ch:"span_count"`
	// Percentiles are filled by the service layer from sketch.Querier.
	P50Ms float64 `ch:"p50_ms"`
	P95Ms float64 `ch:"p95_ms"`
	P99Ms float64 `ch:"p99_ms"`
}

type errorOperationRow struct {
	ServiceName   string  `ch:"service_name"`
	OperationName string  `ch:"operation_name"`
	TotalCount    int64   `ch:"total_count"`
	ErrorCount    int64   `ch:"error_count"`
	ErrorRate     float64 `ch:"error_rate"`
}

// p95LatencyTSRow scans one (timestamp, service_name, span_count) row from
// GetP95LatencyTimeSeries. It is the coverage set: the sketch timeseries
// layered on top in service.go supplies the actual p95 value for each point.
type p95LatencyTSRow struct {
	Timestamp   time.Time `ch:"timestamp"`
	ServiceName string    `ch:"service_name"`
	SpanCount   int64     `ch:"span_count"`
}
