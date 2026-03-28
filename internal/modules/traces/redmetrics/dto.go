package redmetrics

type redSummaryServiceRow struct {
	ServiceName string  `ch:"service_name"`
	TotalCount  int64   `ch:"total_count"`
	ErrorCount  int64   `ch:"error_count"`
	P50Ms       float64 `ch:"p50_ms"`
	P95Ms       float64 `ch:"p95_ms"`
	P99Ms       float64 `ch:"p99_ms"`
}

type apdexRow struct {
	ServiceName string `ch:"service_name"`
	Satisfied   int64  `ch:"satisfied"`
	Tolerating  int64  `ch:"tolerating"`
	Frustrated  int64  `ch:"frustrated"`
	TotalCount  int64  `ch:"total_count"`
}

type latencyBreakdownRow struct {
	ServiceName string  `ch:"service_name"`
	TotalMs     float64 `ch:"total_ms"`
	SpanCount   int64   `ch:"span_count"`
}

type slowOperationDTO = SlowOperation
type errorOperationDTO = ErrorOperation
type serviceRatePointDTO = ServiceRatePoint
type serviceErrorRatePointDTO = ServiceErrorRatePoint
type serviceLatencyPointDTO = ServiceLatencyPoint
type spanKindPointDTO = SpanKindPoint
type errorByRoutePointDTO = ErrorByRoutePoint
