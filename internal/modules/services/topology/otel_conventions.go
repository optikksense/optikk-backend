package topology

// Raw ClickHouse column references for observability.spans (aliased as s)
// and observability.resources (aliased as r). All queries in this module
// use FROM observability.spans s ANY LEFT JOIN observability.resources r
// ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint.

const (
	// Service Health Thresholds (error rate percentages)
	HealthyMaxErrorRate   = 1.0
	DegradedMaxErrorRate  = 5.0
	UnhealthyMinErrorRate = 5.0

	// Service Status Values
	StatusHealthy   = "healthy"
	StatusDegraded  = "degraded"
	StatusUnhealthy = "unhealthy"

	// Query Limits
	MaxEdges = 100
)

// ErrorCondition returns the SQL condition for identifying errors using raw schema columns.
func ErrorCondition() string {
	return "s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400"
}

// RootSpanCondition returns the SQL condition for filtering root spans.
func RootSpanCondition() string {
	return "s.parent_span_id = ''"
}
