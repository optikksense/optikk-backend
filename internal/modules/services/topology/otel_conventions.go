package topology

// OpenTelemetry Semantic Conventions for Service Topology
// Based on OpenTelemetry Semantic Conventions for Traces
// Reference: https://opentelemetry.io/docs/specs/semconv/

const (
	// Span Table Columns - Standard OpenTelemetry fields
	ColTeamID            = "team_id"
	ColServiceName       = "service_name"
	ColParentServiceName = "parent_service_name"
	ColStartTime         = "start_time"
	ColDurationMs        = "duration_ms"
	ColStatus            = "status"
	ColHTTPStatusCode    = "http_status_code"
	ColIsRoot            = "is_root"

	// Status Values - OpenTelemetry Span Status
	StatusOK    = "OK"
	StatusError = "ERROR"

	// HTTP Status Code Thresholds
	HTTPErrorThreshold = 400

	// Service Health Thresholds (error rate percentages)
	HealthyMaxErrorRate   = 1.0
	DegradedMaxErrorRate  = 5.0
	UnhealthyMinErrorRate = 5.0

	// Service Status Values
	StatusHealthy   = "healthy"
	StatusDegraded  = "degraded"
	StatusUnhealthy = "unhealthy"

	// Metric Aggregation Functions
	AggCount   = "count()"
	AggCountIf = "countIf"
	AggAvg     = "avg"

	// Query Limits
	MaxEdges = 100
)

// ErrorCondition returns the SQL condition for identifying errors based on OpenTelemetry conventions
func ErrorCondition() string {
	return "status = 'ERROR' OR http_status_code >= 400"
}

// RootSpanCondition returns the SQL condition for filtering root spans
func RootSpanCondition() string {
	return "is_root = 1"
}

