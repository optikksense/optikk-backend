package database

import "fmt"

// OpenTelemetry Semantic Conventions for Database Metrics
// Reference: https://opentelemetry.io/docs/specs/semconv/database/

const (
	// Operation duration histogram — primary metric for query observability.
	MetricDBOperationDuration = "db.client.operation.duration"

	// Connection pool gauges.
	MetricDBConnectionCount      = "db.client.connection.count"
	MetricDBConnectionMax        = "db.client.connection.max"
	MetricDBConnectionIdleMax    = "db.client.connection.idle.max"
	MetricDBConnectionIdleMin    = "db.client.connection.idle.min"
	MetricDBConnectionPendReqs   = "db.client.connection.pending_requests"
	MetricDBConnectionTimeouts   = "db.client.connection.timeouts"
	MetricDBConnectionCreateTime = "db.client.connection.create_time"
	MetricDBConnectionWaitTime   = "db.client.connection.wait_time"
	MetricDBConnectionUseTime    = "db.client.connection.use_time"

	// Span / metric attributes.
	AttrDBSystem         = "db.system"
	AttrDBNamespace      = "db.namespace"
	AttrDBOperationName  = "db.operation.name"
	AttrDBCollectionName = "db.collection.name"
	AttrDBQueryText      = "db.query.text"
	AttrDBResponseStatus = "db.response.status_code"
	AttrErrorType        = "error.type"
	AttrServerAddress    = "server.address"
	AttrServerPort       = "server.port"
	AttrPoolName         = "pool.name"
	AttrConnectionState  = "db.client.connection.state"

	// ClickHouse table / column names (shared with other modules).
	TableMetrics  = "observability.metrics"
	ColMetricName = "metric_name"
	ColTeamID     = "team_id"
	ColTimestamp  = "timestamp"
	ColValue      = "value"
)

// attrString returns the ClickHouse expression to read a specific attribute string field.
func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
