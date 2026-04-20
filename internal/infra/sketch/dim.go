package sketch

import "strings"

// Dim helpers are the single source of truth for the dimension strings used
// as sketch keys. Ingest consumers call them once per record; repositories
// call them when constructing query dims. Any change to a dim tuple here
// must be reflected in both paths; drift means sketch lookups silently miss.
//
// Empty segments are preserved ("||" stays "||") so dim strings remain stable
// across sparse fields — merging sketches across "a||" and "a|x|" is safe,
// silently skipping the key as if it never existed is not.

const dimSep = "|"

// DimSpanService returns the dim string for SpanLatencyService.
func DimSpanService(serviceName string) string {
	return serviceName
}

// DimSpanEndpoint returns the dim string for SpanLatencyEndpoint. Tuple is
// (service_name, operation_name, endpoint, http_method).
func DimSpanEndpoint(serviceName, operationName, endpoint, method string) string {
	return joinDim(serviceName, operationName, endpoint, method)
}

// DimDbOp returns the dim string for DbOpLatency. Tuple is
// (db_system, operation, collection, namespace).
func DimDbOp(system, operation, collection, namespace string) string {
	return joinDim(system, operation, collection, namespace)
}

// DimDbQuery returns the dim string for DbQueryLatency. Tuple is
// (db_system, query_text_fingerprint). Separate from DimDbOp so slowqueries'
// high-card fingerprint dim can be memory-capped independently.
func DimDbQuery(system, queryTextFingerprint string) string {
	return joinDim(system, queryTextFingerprint)
}

// DimKafkaTopic returns the dim string for KafkaTopicLatency.
func DimKafkaTopic(topic, clientID string) string {
	return joinDim(topic, clientID)
}

// DimHttpServer returns the dim string for HttpServerDuration.
func DimHttpServer(scope, route, method, statusCode, host string) string {
	return joinDim(scope, route, method, statusCode, host)
}

// DimHttpClient returns the dim string for HttpClientDuration.
func DimHttpClient(scope, hostTarget, method, statusCode string) string {
	return joinDim(scope, hostTarget, method, statusCode)
}

// DimJvmMetric returns the dim string for JvmMetricLatency.
func DimJvmMetric(metricName, serviceName, pod string) string {
	return joinDim(metricName, serviceName, pod)
}

// DimHost returns the dim string for NodePodCount — not the element, the
// grouping key. Elements (pod names) are passed separately to ObserveIdentity.
func DimHost(host, namespace string) string {
	return joinDim(host, namespace)
}

// IsEmptyDim returns true when every segment of the dim string is blank —
// observers should skip such records to avoid a single "||...|" key that
// collects everything un-attributed.
func IsEmptyDim(dim string) bool {
	return strings.ReplaceAll(dim, dimSep, "") == ""
}

func joinDim(parts ...string) string {
	// Pre-size: sum of lengths + separators.
	n := len(parts) - 1
	for _, p := range parts {
		n += len(p)
	}
	var b strings.Builder
	b.Grow(n)
	for i, p := range parts {
		if i > 0 {
			b.WriteString(dimSep)
		}
		b.WriteString(p)
	}
	return b.String()
}
