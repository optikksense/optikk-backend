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

	// Query Limits
	MaxNodes    = 200
	MaxServices = 100

	// Default Values
	DefaultUnknown = "unknown"
)
