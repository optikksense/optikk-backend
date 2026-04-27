// Package schema holds the spans signal's Kafka wire format and ClickHouse column mapping.
//
//go:generate protoc --go_out=. --go_opt=paths=source_relative span_row.proto
package schema

import (
	"strings"
	"time"
)

const CHTable = "observability.spans"

var Columns = []string{
	"ts_bucket", "team_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error", "is_remote",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host", "external_http_url", "external_http_method",
	"response_status_code",
	"service", "host", "pod", "service_version", "environment",
	"peer_service", "db_system", "db_name", "db_statement", "http_route",
	"http_status_bucket",
	"attributes",
	"fingerprint",
	"events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
}

// ChValues returns positional values aligned with Columns for the CH batch insert.
func ChValues(r *Row) []any {
	return []any{
		r.GetTsBucket(),
		r.GetTeamId(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetTraceId(),
		r.GetSpanId(),
		r.GetParentSpanId(),
		r.GetTraceState(),
		r.GetFlags(),
		r.GetName(),
		int8(r.GetKind()), //nolint:gosec // OTLP span kind is 0..5
		r.GetKindString(),
		r.GetDurationNano(),
		r.GetHasError(),
		r.GetIsRemote(),
		int16(r.GetStatusCode()), //nolint:gosec // OTLP status code is 0..2
		r.GetStatusCodeString(),
		r.GetStatusMessage(),
		r.GetHttpUrl(),
		r.GetHttpMethod(),
		r.GetHttpHost(),
		r.GetExternalHttpUrl(),
		r.GetExternalHttpMethod(),
		r.GetResponseStatusCode(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetServiceVersion(),
		r.GetEnvironment(),
		r.GetPeerService(),
		r.GetDbSystem(),
		r.GetDbName(),
		r.GetDbStatement(),
		r.GetHttpRoute(),
		r.GetHttpStatusBucket(),
		r.GetAttributes(),
		r.GetFingerprint(),
		r.GetEvents(),
		r.GetLinks(),
		r.GetExceptionType(),
		r.GetExceptionMessage(),
		r.GetExceptionStacktrace(),
		r.GetExceptionEscaped(),
	}
}

// EndpointDim composes the "service|operation|endpoint|method" tuple for SpanLatencyEndpoint sketches.
func EndpointDim(r *Row) string {
	svc := r.GetService()
	op := r.GetName()
	endpoint := r.GetHttpRoute()
	if endpoint == "" {
		endpoint = firstNonEmpty(r.GetAttributes(), "url.path", "http.target")
	}
	method := r.GetHttpMethod()
	var b strings.Builder
	b.Grow(len(svc) + len(op) + len(endpoint) + len(method) + 3)
	b.WriteString(svc)
	b.WriteByte('|')
	b.WriteString(op)
	b.WriteByte('|')
	b.WriteString(endpoint)
	b.WriteByte('|')
	b.WriteString(method)
	return b.String()
}

// HostDim collapses (host, pod namespace) to a sketch dimension for node-pod cardinality.
func HostDim(r *Row) string {
	host := r.GetHost()
	if host == "" {
		host = firstNonEmpty(r.GetAttributes(), "k8s.node.name", "server.address")
	}
	ns := r.GetAttributes()["k8s.namespace.name"]
	return host + "|" + ns
}

func K8sPodName(r *Row) string {
	return r.GetPod()
}

func firstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}
