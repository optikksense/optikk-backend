package slo

import rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"

// Raw ClickHouse column references for observability.spans (aliased as s)
// and observability.resources (aliased as r). All queries in this module
// use FROM observability.spans s ANY LEFT JOIN observability.resources r
// ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint.

const (
	QuantileP95 = 0.95
)

// ErrorCondition uses the UInt16 alias column http_status_code (a CAST of
// response_status_code that CH resolves automatically) so the emitted SQL is
// a plain column comparison with no function-form status-code cast.
func ErrorCondition() string {
	return "s.has_error = true OR s.http_status_code >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
