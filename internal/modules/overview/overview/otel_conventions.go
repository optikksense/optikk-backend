package overview

// Raw ClickHouse column references for observability.spans (aliased as s).

const (
	ColServiceName   = "s.service_name"
	ColOperationName = "s.name"
	ColHTTPMethod    = "s.http_method"
	ColTraceID       = "s.trace_id"

	// Quantile Values
	QuantileP50 = 0.5
	QuantileP95 = 0.95
	QuantileP99 = 0.99
)

// ErrorCondition returns the SQL condition for identifying errors using raw schema columns.
func ErrorCondition() string {
	return "s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400"
}

// RootSpanCondition returns the SQL condition for filtering root spans.
func RootSpanCondition() string {
	return "s.parent_span_id = ''"
}
