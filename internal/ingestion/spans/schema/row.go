// Package schema holds the spans signal's Kafka wire format and ClickHouse
// column mapping. Row is the protobuf message produced on the ingest topic
// and consumed by the dispatcher; CHTable/Columns/ChValues drive the batch
// insert into observability.signoz_index_v3.
//
//go:generate protoc --go_out=. --go_opt=paths=source_relative span_row.proto
package schema

import (
	"strings"
	"time"
)

// CHTable is the ClickHouse destination table for the span signal — the v2
// observability.signoz_index_v3 — DDL in db/clickhouse/01_spans.sql.
const CHTable = "observability.signoz_index_v3"

// Columns is the insert column order for CHTable. Mirrors Row's proto fields
// one-for-one so ChValues can emit positional values without a lookup.
var Columns = []string{
	"ts_bucket_start", "team_id",
	"timestamp", "trace_id", "span_id", "parent_span_id", "trace_state", "flags",
	"name", "kind", "kind_string", "duration_nano", "has_error", "is_remote",
	"status_code", "status_code_string", "status_message",
	"http_url", "http_method", "http_host", "external_http_url", "external_http_method",
	"response_status_code",
	"attributes", "events", "links",
	"exception_type", "exception_message", "exception_stacktrace", "exception_escaped",
}

// ChValues returns positional values aligned with Columns for CH batch insert.
// Proto uses int32 for what the CH schema stores as int8/int16; OTLP's value
// ranges (kind 0..5, status code 0..2) make the narrowing cast safe.
func ChValues(r *Row) []any {
	return []any{
		r.GetTsBucketStart(),
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
		r.GetAttributes(),
		r.GetEvents(),
		r.GetLinks(),
		r.GetExceptionType(),
		r.GetExceptionMessage(),
		r.GetExceptionStacktrace(),
		r.GetExceptionEscaped(),
	}
}

// ServiceName returns the OTel service.name from row attributes, or "" when
// absent. Mirrors the CH materialized column mat_service_name.
func ServiceName(r *Row) string {
	return r.GetAttributes()["service.name"]
}

// EndpointDim composes the dimension tuple used by SpanLatencyEndpoint
// sketches: service | operation | endpoint | method. Empty segments are
// preserved as empty strings so the dim string is stable across sparse fields.
func EndpointDim(r *Row) string {
	svc := ServiceName(r)
	op := r.GetName()
	endpoint := firstNonEmpty(r.GetAttributes(), "http.route", "url.path", "http.target")
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

// HostDim collapses (host, pod namespace) to a sketch dimension for node-pod
// cardinality. Uses host.name or k8s.node.name as the host identifier.
func HostDim(r *Row) string {
	host := firstNonEmpty(r.GetAttributes(), "host.name", "k8s.node.name", "server.address")
	ns := r.GetAttributes()["k8s.namespace.name"]
	return host + "|" + ns
}

// K8sPodName returns attributes["k8s.pod.name"] or "".
func K8sPodName(r *Row) string {
	return r.GetAttributes()["k8s.pod.name"]
}

func firstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}
