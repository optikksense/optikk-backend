package database

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Ensure timebucket import is used.
var _ = timebucket.Expression

const (
	// DefaultUnknown is used when a dimensional value cannot be extracted.
	DefaultUnknown = "unknown"

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
	ColAttributes  = "attributes"
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
