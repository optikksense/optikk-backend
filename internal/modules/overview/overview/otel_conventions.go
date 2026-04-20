package overview

import rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"

// Raw ClickHouse column references for observability.spans (aliased as s).

const (
	ColServiceName   = "s.service_name"
	ColOperationName = "s.name"
	ColHTTPMethod    = "s.http_method"
	ColTraceID       = "s.trace_id"

	QuantileP50 = 0.5
	QuantileP95 = 0.95
	QuantileP99 = 0.99
)

// ErrorCondition uses the UInt16 alias column http_status_code (a CAST of
// response_status_code that CH resolves automatically) so the predicate stays
// a plain column comparison with no function-form status-code cast.
func ErrorCondition() string {
	return "s.has_error = true OR s.http_status_code >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
