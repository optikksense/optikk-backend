package errors

// OpenTelemetry Semantic Conventions for Error Tracking
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
	ColStatusMessage  = "status_message"
	ColHTTPStatusCode = "http_status_code"
	ColIsRoot         = "is_root"
	ColTraceID        = "trace_id"

	// Status Values - OpenTelemetry Span Status
	StatusOK    = "OK"
	StatusError = "ERROR"

	// HTTP Status Code Thresholds
	HTTPErrorThreshold = 400

	// Metric Aggregation Functions
	AggCount   = "COUNT(*)"
	AggCountIf = "countIf"
	AggAvg     = "avg"
	AggMax     = "MAX"
	AggMin     = "MIN"

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
