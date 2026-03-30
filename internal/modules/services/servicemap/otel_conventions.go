package servicemap

import rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"

const (
	HealthyMaxErrorRate   = 1.0
	DegradedMaxErrorRate  = 5.0
	UnhealthyMinErrorRate = 5.0

	StatusHealthy   = "healthy"
	StatusDegraded  = "degraded"
	StatusUnhealthy = "unhealthy"

	MaxEdges = 100
)

func ErrorCondition() string {
	return "s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400"
}

func RootSpanCondition() string {
	return rootspan.Condition("s")
}
