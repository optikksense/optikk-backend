package nodes

// OpenTelemetry Semantic Conventions for Infrastructure Nodes
// Based on OpenTelemetry Semantic Conventions for Resources
// Reference: https://opentelemetry.io/docs/specs/semconv/resource/

const (
	// Span Table Columns - Standard OpenTelemetry fields
	ColTeamID         = "team_id"
	ColServiceName    = "service_name"
	ColStartTime      = "start_time"
	ColDurationMs     = "duration_ms"
	ColStatus         = "status"
	ColHTTPStatusCode = "http_status_code"
	ColHost           = "host"
	ColPod            = "pod"
	ColContainer      = "container"

	// Resource Attributes - OpenTelemetry Semantic Conventions
	AttrHostName = "host.name"

	// Status Values - OpenTelemetry Span Status
	StatusOK    = "OK"
	StatusError = "ERROR"

	// HTTP Status Code Thresholds
	HTTPErrorThreshold = 400

	// Metric Aggregation Functions
	AggCount             = "COUNT(*)"
	AggSum               = "sum"
	AggAvg               = "AVG"
	AggQuantile          = "quantile"
	AggUniqExact         = "uniqExact"
	AggUniqExactIf       = "uniqExactIf"
	AggMax               = "MAX"
	AggGroupUniqArray    = "groupUniqArray"
	AggArrayStringConcat = "arrayStringConcat"

	// Quantile Values
	QuantileP95 = 0.95

	// Query Limits
	MaxNodes    = 200
	MaxServices = 100

	// Default Values
	DefaultUnknown = "unknown"
)

// ErrorCondition returns the SQL condition for identifying errors based on OpenTelemetry conventions
func ErrorCondition() string {
	return "status='ERROR' OR http_status_code >= 400"
}

// RootSpanCondition returns the SQL condition for filtering root spans
func RootSpanCondition() string {
	return "is_root = 1"
}

// HostNameExpression returns the SQL expression for extracting host name
func HostNameExpression() string {
	return "if(host != '', host, ifNull(nullIf(JSONExtractString(attributes, 'host.name'), ''), 'unknown'))"
}
