// Package spans is the span-ingest module. Files are flat: producer.go,
// consumer.go, livetail.go, handler.go, mapper.go (+ mapper_attrs.go,
// mapper_status.go), and row.go live side-by-side so the full pipeline for
// one signal is obvious.
//
// Regenerate row.pb.go after editing row.proto:
//
//	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative row.proto
package spans

import "time"

// CHTable is the ClickHouse destination table for the span signal.
const CHTable = "observability.spans"

// Columns is the insert column order for CHTable. Mirrors Row's proto fields
// one-for-one so chValues can emit positional values without a lookup.
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

// chValues returns positional values aligned with Columns for CH batch insert.
// Proto uses int32 for what the CH schema stores as int8/int16; OTLP's value
// ranges (kind 0..5, status code 0..2) make the narrowing cast safe.
func chValues(r *Row) []any {
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
