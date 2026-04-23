// Package logs is the log-ingest module. Files in this directory are flat by
// design: producer.go, consumer.go, handler.go, mapper.go, and
// row.go sit side-by-side so the full pipeline for one signal is visible at a
// glance.
//
// Regenerate row.pb.go after editing row.proto:
//
//	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative row.proto
//go:generate protoc --go_out=. --go_opt=paths=source_relative log_row.proto
package logs

import "time"

// CHTable is the ClickHouse destination table for the log signal — the v2
// observability.logs — DDL in db/clickhouse/02_logs.sql.
const CHTable = "observability.logs"

// Columns is the insert column order for CHTable. Mirrors Row's proto fields
// one-for-one so chValues can emit positional values without a lookup. The
// legacy `scope_string` column was dropped in the ingest rewrite.
var Columns = []string{
	"team_id", "ts_bucket_start", "timestamp", "observed_timestamp",
	"trace_id", "span_id", "trace_flags", "severity_text", "severity_number", "body",
	"attributes_string", "attributes_number", "attributes_bool",
	"resource", "resource_fingerprint",
	"scope_name", "scope_version",
}

// chValues returns positional values aligned with Columns for CH batch insert.
// Narrowing casts handle proto's lack of int8/int16/uint8 — the source OTLP
// data is bounded by spec so overflow cannot occur.
func chValues(r *Row) []any {
	return []any{
		r.GetTeamId(),
		r.GetTsBucketStart(),
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
		r.GetResourceFingerprint(),
		r.GetScopeName(),
		r.GetScopeVersion(),
	}
}
