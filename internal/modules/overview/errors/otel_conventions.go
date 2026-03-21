package errors

import rootspan "github.com/observability/observability-backend-go/internal/modules/spans/shared/rootspan"

// Raw ClickHouse column references for observability.spans (aliased as s)
// and observability.resources (aliased as r). All queries in this module
// use FROM observability.spans s ANY LEFT JOIN observability.resources r
// ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint.

func ErrorCondition() string {
	return "s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
