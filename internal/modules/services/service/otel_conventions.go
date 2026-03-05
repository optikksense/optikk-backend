package servicepage

// OpenTelemetry Semantic Conventions for Service Metrics
// Based on OpenTelemetry Semantic Conventions for Traces
// Reference: https://opentelemetry.io/docs/specs/semconv/

const (
	// Span Table Columns - Standard OpenTelemetry fields
	ColTeamID         = "team_id"
	ColServiceName    = "service_name"
	ColOperationName  = "operation_name"
	ColStartTime      = "start_time"
	ColDurationMs     = "duration_ms"
	ColStatus         = "status"
	ColHTTPStatusCode = "http_status_code"
	ColHTTPMethod     = "http_method"
	ColIsRoot         = "is_root"

	// Status Values - OpenTelemetry Span Status
	StatusOK    = "OK"
	StatusError = "ERROR"

	// HTTP Status Code Thresholds
	HTTPErrorThreshold = 400

	// Service Health Thresholds (error rate percentages)
	HealthyMaxErrorRate  = 1.0
	DegradedMaxErrorRate = 5.0

	// Metric Aggregation Functions
	AggCount    = "count()"
	AggCountIf  = "countIf"
	AggAvg      = "avg"
	AggQuantile = "quantile"

	// Quantile Values
	QuantileP50 = 0.5
	QuantileP95 = 0.95
	QuantileP99 = 0.99

	// Time Bucketing Intervals (in milliseconds)
	ThreeHours      = 3 * 3_600_000
	TwentyFourHours = 24 * 3_600_000
	OneWeek         = 168 * 3_600_000
)

// ErrorCondition returns the SQL condition for identifying errors based on OpenTelemetry conventions
func ErrorCondition() string {
	return "status = 'ERROR' OR http_status_code >= 400"
}

// RootSpanCondition returns the SQL condition for filtering root spans
func RootSpanCondition() string {
	return "is_root = 1"
}
