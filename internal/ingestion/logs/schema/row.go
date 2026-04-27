// Package schema holds the logs signal's Kafka wire format and ClickHouse column mapping.
//
//go:generate protoc --go_out=. --go_opt=paths=source_relative log_row.proto
package schema

import "time"

const CHTable = "observability.logs"

var Columns = []string{
	"team_id", "ts_bucket", "timestamp", "observed_timestamp",
	"trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "body",
	"attributes_string", "attributes_number", "attributes_bool",
	"resource", "fingerprint",
	"scope_name", "scope_version",
	"service", "host", "pod", "container", "environment",
}

func ChValues(r *Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetTsBucket(),
		time.Unix(0, r.GetTimestampNs()),
		r.GetObservedTimestampNs(),
		r.GetTraceId(),
		r.GetSpanId(),
		r.GetTraceFlags(),
		r.GetSeverityText(),
		uint8(r.GetSeverityNumber()), //nolint:gosec // OTLP severity is 0..24
		r.GetBody(),
		r.GetAttributesString(),
		r.GetAttributesNumber(),
		r.GetAttributesBool(),
		r.GetResource(),
		r.GetFingerprint(),
		r.GetScopeName(),
		r.GetScopeVersion(),
		r.GetService(),
		r.GetHost(),
		r.GetPod(),
		r.GetContainer(),
		r.GetEnvironment(),
	}
}
