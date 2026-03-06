package database

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/dashboardconfig"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Ensure timebucket import is used.
var _ = timebucket.Expression

func init() {
	dashboardconfig.RegisterDefaultConfig("database", defaultDatabase)
	dashboardconfig.RegisterDefaultConfig("redis", defaultRedis)
}

const (
	// DefaultUnknown is used when a dimensional value cannot be extracted.
	DefaultUnknown = "unknown"

	// Table name for the metrics store.
	TableMetrics = "metrics"

	// DB system canonical names
	DBSystemMongoDB = "mongodb"
	DBSystemMySQL   = "mysql"
	DBSystemRedis   = "redis"
	DBSystemSQL     = "sql"

	// Metric Names used in queries
	MetricMongoDBDriverCommands    = "mongodb.driver.commands"
	MetricHikariCPConnectionsUsage = "hikaricp.connections.usage"
	MetricCacheHits                = "db.cache.hits"
	MetricCacheMisses              = "db.cache.misses"
	MetricDBReplicationLagMs       = "db.replication.lag.ms"
	MetricDBClientErrors           = "db.client.errors"

	// Column names
	ColMetricName  = "metric_name"
	ColServiceName = "service"
	ColCount       = "hist_count"
	ColAvg         = "value"
	ColMax         = "value"
	ColP95         = "value"
	ColTeamID      = "team_id"
	ColTimestamp   = "timestamp"
	ColValue       = "value"

	// Constants for queries
	MaxTopTables = 50

	// OTel SemConv overrides/extensions
	AttrCollection          = "collection"
	AttrDBCollectionName    = "db.collection.name"
	AttrDBMongoDBCollection = "db.mongodb.collection"
	AttrDBSQLTable          = "db.sql.table"
	AttrPool                = "pool"
)

var (
	DatabaseLatencyMetrics = []string{
		MetricMongoDBDriverCommands,
		MetricHikariCPConnectionsUsage,
	}

	DatabaseAllMetrics = []string{
		MetricMongoDBDriverCommands,
		MetricHikariCPConnectionsUsage,
		MetricCacheHits,
		MetricCacheMisses,
		MetricDBReplicationLagMs,
		MetricDBClientErrors,
	}
)

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// helper functions for repository queries
func MetricSetToInClause(metrics []string) string {
	res := ""
	for i, m := range metrics {
		if i > 0 {
			res += ", "
		}
		res += "'" + m + "'"
	}
	return res
}

func syncAggregateExpr(expr1, expr2 string) string {
	return "coalesce(" + expr1 + ", " + expr2 + ")"
}

func mergeNullableFloatPair(val1, val2 *float64) *float64 {
	if val1 != nil && val2 != nil {
		avg := (*val1 + *val2) / 2
		return &avg
	}
	if val1 != nil {
		return val1
	}
	return val2
}

func nullableMergedFloatPair(val1, val2 *float64) float64 {
	res := mergeNullableFloatPair(val1, val2)
	if res == nil {
		return 0
	}
	return *res
}

func mergeQueryCount(val1, val2 int64) int64 {
	return val1 + val2
}

const defaultDatabase = `page: database
title: "Database"
icon: "Database"
subtitle: "Query latency, connection pool saturation, and top tables"

dataSources:
  - id: db-latency-summary
    endpoint: /v1/saturation/database/latency-summary
  - id: db-avg-latency
    endpoint: /v1/saturation/database/avg-latency
  - id: db-query-by-table
    endpoint: /v1/saturation/database/query-by-table
  - id: db-systems
    endpoint: /v1/saturation/database/systems
  - id: db-top-tables
    endpoint: /v1/saturation/database/top-tables
  - id: db-connection-count
    endpoint: /v1/saturation/database/connection-count
  - id: db-connection-wait
    endpoint: /v1/saturation/database/connection-wait-time
  - id: db-connection-pending
    endpoint: /v1/saturation/database/connection-pending
  - id: db-connection-timeouts
    endpoint: /v1/saturation/database/connection-timeouts
  - id: db-query-duration
    endpoint: /v1/saturation/database/query-duration

statCards:
  - title: "Avg Query Latency"
    dataSource: db-latency-summary
    valueField: avg_query_latency_ms
    formatter: ms
    icon: Clock
  - title: "p95 Query Latency"
    dataSource: db-latency-summary
    valueField: p95_query_latency_ms
    formatter: ms
    icon: Clock
  - title: "DB Span Count"
    dataSource: db-latency-summary
    valueField: db_span_count
    formatter: number
    icon: Activity
  - title: "Cache Hit Rate"
    dataSource: db-latency-summary
    valueField: cache_hits
    formatter: number
    icon: Database

charts:
  - id: db-avg-latency
    title: "Average Query Latency"
    type: latency
    titleIcon: Clock
    layout:
      col: 12
    dataSource: db-avg-latency
    valueKey: avg_latency_ms
    height: 260
  - id: db-systems
    title: "Database Systems"
    type: table
    titleIcon: Database
    layout:
      col: 12
    dataSource: db-systems
    height: 260
  - id: db-connection-count
    title: "Connection Count by State"
    type: area
    titleIcon: Network
    layout:
      col: 12
    dataSource: db-connection-count
    groupByKey: state
    valueKey: value
    height: 260
  - id: db-connection-pending
    title: "Pending Connection Requests"
    type: area
    titleIcon: Clock
    layout:
      col: 12
    dataSource: db-connection-pending
    valueKey: avg_latency_ms
    height: 260
  - id: db-connection-timeouts
    title: "Connection Timeouts"
    type: area
    titleIcon: AlertTriangle
    layout:
      col: 12
    dataSource: db-connection-timeouts
    valueKey: avg_latency_ms
    height: 260
  - id: db-query-duration
    title: "Query Duration p50/p95/p99"
    type: stat
    titleIcon: Activity
    layout:
      col: 12
    dataSource: db-query-duration
    height: 120
  - id: db-top-tables
    title: "Top Tables by Latency"
    type: table
    titleIcon: Table
    layout:
      col: 24
    dataSource: db-top-tables
    height: 320
  - id: db-query-by-table
    title: "Query Volume by Table"
    type: area
    titleIcon: BarChart
    layout:
      col: 24
    dataSource: db-query-by-table
    groupByKey: table
    valueKey: avg_latency_ms
    height: 280
`

const defaultRedis = `page: redis
title: "Redis"
icon: "Layers"
subtitle: "Redis cache performance, memory, clients, and replication"

dataSources:
  - id: redis-hit-rate
    endpoint: /v1/saturation/redis/cache-hit-rate
  - id: redis-clients
    endpoint: /v1/saturation/redis/clients
  - id: redis-memory
    endpoint: /v1/saturation/redis/memory
  - id: redis-memory-frag
    endpoint: /v1/saturation/redis/memory-fragmentation
  - id: redis-commands
    endpoint: /v1/saturation/redis/commands
  - id: redis-evictions
    endpoint: /v1/saturation/redis/evictions
  - id: redis-keyspace
    endpoint: /v1/saturation/redis/keyspace
  - id: redis-key-expiries
    endpoint: /v1/saturation/redis/key-expiries
  - id: redis-replication-lag
    endpoint: /v1/saturation/redis/replication-lag

statCards:
  - title: "Cache Hit Rate"
    dataSource: redis-hit-rate
    valueField: hit_rate_pct
    formatter: percent1
    icon: Target
  - title: "Cache Hits"
    dataSource: redis-hit-rate
    valueField: hits
    formatter: number
    icon: CheckCircle
  - title: "Cache Misses"
    dataSource: redis-hit-rate
    valueField: misses
    formatter: number
    icon: XCircle
  - title: "Replication Offset"
    dataSource: redis-replication-lag
    valueField: offset
    formatter: number
    icon: GitBranch

charts:
  - id: redis-clients
    title: "Connected Clients"
    type: area
    titleIcon: Users
    layout:
      col: 12
    dataSource: redis-clients
    valueKey: value
    height: 260
  - id: redis-memory
    title: "Memory Used"
    type: area
    titleIcon: HardDrive
    layout:
      col: 12
    dataSource: redis-memory
    valueKey: value
    height: 260
  - id: redis-memory-frag
    title: "Memory Fragmentation Ratio"
    type: area
    titleIcon: AlertTriangle
    layout:
      col: 12
    dataSource: redis-memory-frag
    valueKey: value
    height: 260
  - id: redis-commands
    title: "Commands Processed"
    type: area
    titleIcon: Terminal
    layout:
      col: 12
    dataSource: redis-commands
    valueKey: value
    height: 260
  - id: redis-evictions
    title: "Key Evictions"
    type: area
    titleIcon: Trash2
    layout:
      col: 12
    dataSource: redis-evictions
    valueKey: value
    height: 260
  - id: redis-keyspace
    title: "Keyspace Size by DB"
    type: table
    titleIcon: Database
    layout:
      col: 12
    dataSource: redis-keyspace
    height: 260
  - id: redis-key-expiries
    title: "Key Expiries by DB"
    type: table
    titleIcon: Clock
    layout:
      col: 24
    dataSource: redis-key-expiries
    height: 260
`
