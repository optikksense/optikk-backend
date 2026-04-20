package errors

import rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"

// Raw ClickHouse column references for observability.spans (aliased as s)
// and observability.resources (aliased as r). All queries in this module
// use FROM observability.spans s ANY LEFT JOIN observability.resources r
// ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint.
//
// s.http_status_code is a schema-level ALIAS that parses the raw
// response_status_code string column into a UInt16 server-side;
// referencing the alias keeps user-written SQL free of cast combinators.

func ErrorCondition() string {
	return "s.has_error = true OR s.http_status_code >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
