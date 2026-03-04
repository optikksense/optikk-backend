package database

import "fmt"

// OpenTelemetry Semantic Conventions for Databases
// Reference: https://opentelemetry.io/docs/specs/semconv/database/

const (
	// Database Attributes
	AttrDBSystem    = "db.system"
	AttrDBName      = "db.name"
	AttrDBStatement = "db.statement"
	AttrDBOperation = "db.operation"
	AttrDBTable     = "db.table"
	AttrDBSqlTable  = "db.sql.table"
	AttrDBUser      = "db.user"

	// Span Types & Categories
	SpanKindClient = "SPAN_KIND_CLIENT"
	SpanKindServer = "SPAN_KIND_SERVER"

	// Metric Names
	MetricDBCacheHits   = "db.client.response.cache_hits"
	MetricDBCacheMisses = "db.client.response.cache_misses"

	// Table column names for queries
	ColTraceID        = "TraceId"
	ColSpanID         = "SpanId"
	ColSpanName       = "SpanName"
	ColSpanKind       = "SpanKind"
	ColDuration       = "Duration"
	ColSpanStatus     = "StatusCode"
	ColSpanAttributes = "SpanAttributes"

	// Status Values
	StatusOk    = "STATUS_CODE_OK"
	StatusError = "STATUS_CODE_ERROR"
	StatusUnset = "STATUS_CODE_UNSET"

	TableSpans = "otel_traces"
)

// ExtractJSONString builds a ClickHouse JSON extraction expression.
func ExtractJSONString(column, key string) string {
	return fmt.Sprintf("JSONExtractString(%s, '%s')", column, key)
}

// DbSystemCondition builds the condition to verify a span relates to a database.
func DbSystemCondition() string {
	return fmt.Sprintf("length(%s) > 0 AND %s = '%s'",
		ExtractJSONString(ColSpanAttributes, AttrDBSystem),
		ColSpanKind, SpanKindClient)
}
