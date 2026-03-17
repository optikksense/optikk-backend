package shared

import "fmt"

const (
	MetricDBOperationDuration = "db.client.operation.duration"

	MetricDBConnectionCount      = "db.client.connection.count"
	MetricDBConnectionMax        = "db.client.connection.max"
	MetricDBConnectionIdleMax    = "db.client.connection.idle.max"
	MetricDBConnectionIdleMin    = "db.client.connection.idle.min"
	MetricDBConnectionPendReqs   = "db.client.connection.pending_requests"
	MetricDBConnectionTimeouts   = "db.client.connection.timeouts"
	MetricDBConnectionCreateTime = "db.client.connection.create_time"
	MetricDBConnectionWaitTime   = "db.client.connection.wait_time"
	MetricDBConnectionUseTime    = "db.client.connection.use_time"

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

	TableMetrics  = "observability.metrics"
	ColMetricName = "metric_name"
	ColTeamID     = "team_id"
	ColTimestamp  = "timestamp"
	ColValue      = "value"
)

func AttrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
