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

	// Metric Names
	MetricDBCacheHits   = "db.client.response.cache_hits"
	MetricDBCacheMisses = "db.client.response.cache_misses"
)

// attrString returns a CH 26+ native JSON path expression that reads a String
// from the attributes JSON column. Replaces JSONExtractString(attributes, 'key').
func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
