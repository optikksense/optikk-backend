package trace_shape //nolint:revive,stylecheck

type spanKindDurationRow struct {
	SpanKind    string  `ch:"span_kind"`
	TotalDuraMs float64 `ch:"total_duration_ms"`
	SpanCount   int64   `ch:"span_count"`
}

type flamegraphRow struct {
	SpanID        string  `ch:"span_id"`
	ParentSpanID  string  `ch:"parent_span_id"`
	OperationName string  `ch:"operation_name"`
	ServiceName   string  `ch:"service_name"`
	SpanKind      string  `ch:"span_kind"`
	DurationMs    float64 `ch:"duration_ms"`
	StartNs       int64   `ch:"start_ns"`
	HasError      bool    `ch:"has_error"`
}
