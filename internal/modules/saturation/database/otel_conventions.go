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

	// OTel db.client.* standard metrics
	MetricDBClientConnectionCount    = "db.client.connection.count"
	MetricDBClientConnectionWait     = "db.client.connection.wait_time"
	MetricDBClientConnectionPending  = "db.client.connection.pending_requests"
	MetricDBClientConnectionTimeouts = "db.client.connection.timeouts"
	MetricDBClientOperationDuration  = "db.client.operation.duration"

	// Connection attributes
	AttrDBConnectionState = "db.client.connection.state"
	AttrDBOperationName   = "db.operation.name"
	AttrDBPoolName        = "db.client.connection.pool.name"

	// Redis metrics (vendor-specific OTel receivers)
	MetricRedisKeyspaceHits             = "redis.keyspace.hits"
	MetricRedisKeyspaceMisses           = "redis.keyspace.misses"
	MetricRedisClientsConnected         = "redis.clients.connected"
	MetricRedisMemoryUsed               = "redis.memory.used"
	MetricRedisMemoryFragmentationRatio = "redis.memory.fragmentation_ratio"
	MetricRedisCommandsProcessed        = "redis.commands.processed"
	MetricRedisKeysEvicted              = "redis.keys.evicted"
	MetricRedisDBKeys                   = "redis.db.keys"
	MetricRedisDBExpires                = "redis.db.expires"
	MetricRedisReplicationOffset        = "redis.replication.offset"
	MetricRedisReplicationBacklogOffset = "redis.replication.backlog_first_byte_offset"

	// Redis attributes
	AttrRedisDB = "redis.db"
)

// attrString returns a CH 26+ native JSON path expression that reads a String
// from the attributes JSON column. Replaces JSONExtractString(attributes, 'key').
func attrString(attrName string) string {
	return fmt.Sprintf("attributes.'%s'::String", attrName)
}
