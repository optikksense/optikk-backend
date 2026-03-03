package slo

// OpenTelemetry Semantic Conventions for SLO Tracking
// Based on OpenTelemetry Semantic Conventions for Traces
// Reference: https://opentelemetry.io/docs/specs/semconv/

const (
	// Span Table Columns - Standard OpenTelemetry fields
	ColTeamID         = "team_id"
	ColServiceName    = "service_name"
	ColStartTime      = "start_time"
	ColDurationMs     = "duration_ms"
	ColStatus         = "status"
	ColHTTPStatusCode = "http_status_code"
	ColIsRoot         = "is_root"

	// Status Values - OpenTelemetry Span Status
	StatusOK    = "OK"
	StatusError = "ERROR"

	// HTTP Status Code Thresholds
	HTTPErrorThreshold = 400

	// Metric Aggregation Functions
	AggCount   = "count()"
	AggCountIf = "countIf"
	AggAvg     = "avg"
	AggQuantile = "quantile"

	// Quantile Values
	QuantileP95 = 0.95

	// Time Bucketing Intervals (in milliseconds)
	ThreeHours      = 3 * 3_600_000
	TwentyFourHours = 24 * 3_600_000
	OneWeek         = 168 * 3_600_000

	// Time Bucket Functions with Formatting
	FmtIntervalOneMinute    = "formatDateTime(toStartOfMinute(start_time), '%Y-%m-%d %H:%i:00')"
	FmtIntervalFiveMinutes  = "formatDateTime(toStartOfFiveMinutes(start_time), '%Y-%m-%d %H:%i:00')"
	FmtIntervalSixtyMinutes = "formatDateTime(toStartOfHour(start_time), '%Y-%m-%d %H:%i:00')"
	FmtIntervalOneDay       = "formatDateTime(toStartOfDay(start_time), '%Y-%m-%d %H:%i:00')"
)

// ErrorCondition returns the SQL condition for identifying errors based on OpenTelemetry conventions
func ErrorCondition() string {
	return "status = 'ERROR' OR http_status_code >= 400"
}

// RootSpanCondition returns the SQL condition for filtering root spans
func RootSpanCondition() string {
	return "is_root = 1"
}

