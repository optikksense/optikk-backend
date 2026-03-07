package overview

// Raw ClickHouse column references for observability.spans (aliased as s)
// and observability.resources (aliased as r). All queries in this module
// use FROM observability.spans s ANY LEFT JOIN observability.resources r
// ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint.

const (
	ColServiceName   = "r.service_name"
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
	return "s.has_error = true OR s.response_status_code >= '400'"
}

// RootSpanCondition returns the SQL condition for filtering root spans.
func RootSpanCondition() string {
	return "s.parent_span_id = ''"
}
