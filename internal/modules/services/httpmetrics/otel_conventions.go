package httpmetrics

// OpenTelemetry Semantic Conventions for HTTP Metrics
// Reference: https://opentelemetry.io/docs/specs/semconv/http/

const (
	MetricHTTPServerRequestDuration  = "http.server.request.duration"
	MetricHTTPServerActiveRequests   = "http.server.active_requests"
	MetricHTTPServerRequestBodySize  = "http.server.request.body.size"
	MetricHTTPServerResponseBodySize = "http.server.response.body.size"
	MetricHTTPClientRequestDuration  = "http.client.request.duration"
	MetricDNSLookupDuration          = "dns.lookup.duration"
	MetricTLSConnectDuration         = "tls.connect.duration"
)
