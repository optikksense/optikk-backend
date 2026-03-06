package httpmetrics

import "fmt"

// OpenTelemetry Semantic Conventions for HTTP Metrics
// Reference: https://opentelemetry.io/docs/specs/semconv/http/

const (
	// Metric Names
	MetricHTTPServerRequestDuration  = "http.server.request.duration"
	MetricHTTPServerActiveRequests   = "http.server.active_requests"
	MetricHTTPServerRequestBodySize  = "http.server.request.body.size"
	MetricHTTPServerResponseBodySize = "http.server.response.body.size"
	MetricHTTPClientRequestDuration  = "http.client.request.duration"
	MetricDNSLookupDuration          = "dns.lookup.duration"
	MetricTLSConnectDuration         = "tls.connect.duration"

	// Attribute Names
	AttrHTTPStatusCode = "http.response.status_code"
	AttrHTTPMethod     = "http.request.method"
	AttrHTTPRoute      = "http.route"

	// Table Name
	TableMetrics = "metrics"

	// Column Names
	ColMetricName = "metric_name"
	ColTeamID     = "team_id"
	ColTimestamp  = "timestamp"
	ColValue      = "value"
)

// attrString returns a CH 26+ native JSON path expression for a String attribute.
func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
