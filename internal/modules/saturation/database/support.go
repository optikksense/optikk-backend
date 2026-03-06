package database

import (
	"math"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

const (
	// DefaultUnknown is used when a dimensional value cannot be extracted.
	DefaultUnknown = "unknown"

	// Table name for the metrics store.
	TableMetrics = "metrics"

	// DB system canonical names.
	DBSystemMongoDB = "mongodb"
	DBSystemMySQL   = "mysql"
	DBSystemRedis   = "redis"
	DBSystemSQL     = "sql"

	// Metric Names used in queries.
	MetricMongoDBDriverCommands    = "mongodb.driver.commands"
	MetricHikariCPConnectionsUsage = "hikaricp.connections.usage"
	MetricCacheHits                = "db.cache.hits"
	MetricCacheMisses              = "db.cache.misses"
	MetricDBReplicationLagMs       = "db.replication.lag.ms"
	MetricDBClientErrors           = "db.client.errors"

	// Column names.
	ColMetricName  = "metric_name"
	ColServiceName = "service"
	ColCount       = "hist_count"
	ColAvg         = "value"
	ColMax         = "value"
	ColP95         = "value"
	ColTeamID      = "team_id"
	ColTimestamp   = "timestamp"
	ColValue       = "value"

	// Limits.
	MaxTopTables = 50

	// OTel SemConv overrides/extensions.
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

// ClickHouseRepository encapsulates database saturation data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new saturation database repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// MetricSetToInClause formats a quoted IN list for ClickHouse SQL.
func MetricSetToInClause(metrics []string) string {
	res := ""
	for i, metric := range metrics {
		if i > 0 {
			res += ", "
		}
		res += "'" + metric + "'"
	}
	return res
}

func syncAggregateExpr(expr1, expr2 string) string {
	return "coalesce(" + expr1 + ", " + expr2 + ")"
}

func finiteOrNil(v *float64) *float64 {
	if v == nil {
		return nil
	}
	if math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}

func mergeNullableFloatPair(val1, val2 *float64) *float64 {
	val1 = finiteOrNil(val1)
	val2 = finiteOrNil(val2)

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
