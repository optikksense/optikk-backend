package slo

import rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"

// Raw ClickHouse column references for observability.signoz_index_v3 (aliased as s)
// and observability.resources (aliased as r). All queries in this module
// use FROM observability.signoz_index_v3 s ANY LEFT JOIN observability.resources r
// ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint.

const (
	QuantileP95 = 0.95
)

func ErrorCondition() string {
	return "s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
