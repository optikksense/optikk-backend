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

func ErrorCondition() string {
	return "s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
