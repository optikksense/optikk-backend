package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// ──────────────────────────────────────────────────────────────────────────────
// SQL expression builders — Database
// ──────────────────────────────────────────────────────────────────────────────

// dbCollectionExpression builds a COALESCE chain that resolves the canonical
// collection/table name from OTel database attributes. Ordered newest-spec-first,
// with legacy vendor variants as fallbacks.
func dbCollectionExpression() string {
	return fmt.Sprintf(`coalesce(
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	'%s'
)`,
		ColAttributes, AttrDBMongoDBCollection,
		ColAttributes, AttrCollection,
		ColAttributes, AttrDBCollectionName,
		ColAttributes, AttrDBSQLTable,
		ColAttributes, AttrPool,
		ColAttributes, AttrDBStatement,
		DefaultUnknown)
}

// dbSystemExpression resolves the database engine (mongodb, mysql, redis, sql)
// using, in order: the `db.system` attribute, metric-name pattern matching, and
// service-name pattern matching. The nested ifs mirror ClickHouse's lack of a
// CASE WHEN; they are structured for readability with one branch per DB system.
func dbSystemExpression() string {
	return fmt.Sprintf(`coalesce(
	nullIf(JSONExtractString(%s, '%s'), ''),
	if(
		lower(%s) LIKE '%%%%mongo%%%%' OR lower(%s) LIKE '%%%%mongo%%%%',
		'%s',
		if(
			lower(%s) LIKE 'hikaricp.%%%%' OR lower(%s) LIKE 'jdbc.%%%%'
			OR lower(%s) LIKE '%%%%mysql%%%%' OR lower(%s) LIKE '%%%%postgres%%%%',
			'%s',
			if(
				lower(%s) LIKE '%%%%redis%%%%' OR lower(%s) LIKE '%%%%redis%%%%',
				'%s',
				if(
					lower(%s) LIKE '%%%%sql%%%%',
					'%s',
					'%s'
				)
			)
		)
	)
)`,
		ColAttributes, AttrDBSystem,
		ColMetricName, ColServiceName, DBSystemMongoDB,
		ColMetricName, ColMetricName,
		ColServiceName, ColServiceName, DBSystemMySQL,
		ColMetricName, ColServiceName, DBSystemRedis,
		ColMetricName, DBSystemSQL,
		DefaultUnknown)
}

// dbLatencyMetricFilter restricts rows to metrics that carry meaningful latency
// histograms (avg / p95 columns populated).
func dbLatencyMetricFilter() string {
	return fmt.Sprintf(`%s IN (%s)`, ColMetricName, MetricSetToInClause(DatabaseLatencyMetrics))
}

// dbAllMetricFilter is a broader filter covering latency, connection pools,
// cache, replication, and error metrics used in the cache-summary and system
// breakdown queries.
func dbAllMetricFilter() string {
	return fmt.Sprintf(`%s IN (%s)`, ColMetricName, MetricSetToInClause(DatabaseAllMetrics))
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared SQL fragment helpers
// ──────────────────────────────────────────────────────────────────────────────

// mongoLatencyAvg returns an avgIf fragment for a MongoDB latency column,
// converting seconds → milliseconds. isFinite guards against Inf/NaN.
func mongoLatencyAvg(col string) string {
	return fmt.Sprintf(`avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000`,
		col, ColMetricName, MetricMongoDBDriverCommands, col)
}

// hikariLatencyAvg returns an avgIf fragment for a HikariCP connection-usage
// latency column, converting seconds → milliseconds.
func hikariLatencyAvg(col string) string {
	return fmt.Sprintf(`avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000`,
		col, ColMetricName, MetricHikariCPConnectionsUsage, col)
}

// mongoLatencyMax returns a maxIf fragment for a MongoDB latency column.
func mongoLatencyMax(col string) string {
	return fmt.Sprintf(`maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000`,
		col, ColMetricName, MetricMongoDBDriverCommands, col)
}

// hikariLatencyMax returns a maxIf fragment for a HikariCP latency column.
func hikariLatencyMax(col string) string {
	return fmt.Sprintf(`maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000`,
		col, ColMetricName, MetricHikariCPConnectionsUsage, col)
}

// ──────────────────────────────────────────────────────────────────────────────
// Scan-layer helpers
// ──────────────────────────────────────────────────────────────────────────────

// mergeAvgLatencyMs combines a MongoDB-sourced count and a HikariCP-sourced count
// into a single representative value. When both are populated, their average is
// taken; when only one is non-zero, it is used directly.
func mergeAvgLatencyMs(mongo, usage int64) int64 {
	switch {
	case mongo > 0 && usage > 0:
		return (mongo + usage) / 2
	case usage > 0:
		return usage
	default:
		return mongo
	}
}

// nullableFloat64FromAny converts an interface{} (usually *float64) to a float64.
func nullableFloat64FromAny(val interface{}) float64 {
	switch v := val.(type) {
	case *float64:
		if v != nil {
			return *v
		}
	case float64:
		return v
	case *float32:
		if v != nil {
			return float64(*v)
		}
	case float32:
		return float64(v)
	}
	return 0
}

// ──────────────────────────────────────────────────────────────────────────────
// Database repository methods
// ──────────────────────────────────────────────────────────────────────────────

// GetDatabaseQueryByTable returns per-collection/table query counts and latency
// percentiles, bucketed by time. Merges MongoDB driver and HikariCP pool metrics.
func (r *ClickHouseRepository) GetDatabaseQueryByTable(teamUUID string, startMs, endMs int64) ([]DatabaseQueryByTable, error) {
	bucket := timebucket.Expression(startMs, endMs)
	collExpr := dbCollectionExpression()

	query := fmt.Sprintf(`
		SELECT
		    %s                                                              AS table_name,
		    %s                                                              AS minute_bucket,
		    maxIf(%s, %s = '%s')                                            AS mongo_query_count,
		    maxIf(%s, %s = '%s')                                            AS usage_query_count,
		    %s                                                              AS mongo_avg_latency_ms,
		    %s                                                              AS usage_avg_latency_ms,
		    %s                                                              AS mongo_p95_latency_ms,
		    %s                                                              AS usage_p95_latency_ms
		FROM metrics_v5
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY table_name, minute_bucket
		ORDER BY minute_bucket ASC, table_name ASC
	`,
		collExpr, bucket,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		mongoLatencyAvg(ColAvg),
		hikariLatencyAvg(ColAvg),
		mongoLatencyMax(ColP95),
		hikariLatencyMax(ColP95),
		ColTeamID,
		ColTimestamp,
		dbLatencyMetricFilter(),
	)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	results := make([]DatabaseQueryByTable, len(rows))
	for i, row := range rows {
		mongoAvg := dbutil.NullableFloat64FromAny(row["mongo_avg_latency_ms"])
		usageAvg := dbutil.NullableFloat64FromAny(row["usage_avg_latency_ms"])
		mongoP95 := dbutil.NullableFloat64FromAny(row["mongo_p95_latency_ms"])
		usageP95 := dbutil.NullableFloat64FromAny(row["usage_p95_latency_ms"])

		results[i] = DatabaseQueryByTable{
			Table:        dbutil.StringFromAny(row["table_name"]),
			Timestamp:    dbutil.StringFromAny(row["minute_bucket"]),
			QueryCount:   mergeQueryCount(dbutil.Int64FromAny(row["mongo_query_count"]), dbutil.Int64FromAny(row["usage_query_count"])),
			AvgLatencyMs: nullableMergedFloatPair(mongoAvg, usageAvg),
			P95LatencyMs: nullableMergedFloatPair(mongoP95, usageP95),
		}
	}
	return results, nil
}

// GetDatabaseAvgLatency returns the overall (cross-collection) average and p95
// query latency bucketed by time, merging MongoDB driver and HikariCP sources.
func (r *ClickHouseRepository) GetDatabaseAvgLatency(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	bucket := timebucket.Expression(startMs, endMs)

	avgLatencyExpr := syncAggregateExpr(mongoLatencyAvg(ColAvg), hikariLatencyAvg(ColAvg))
	p95LatencyExpr := syncAggregateExpr(mongoLatencyMax(ColP95), hikariLatencyMax(ColP95))

	query := fmt.Sprintf(`
		SELECT
		    %s                                 AS minute_bucket,
		    coalesce(%s, 0)                    AS avg_latency_ms,
		    coalesce(%s, 0)                    AS p95_latency_ms
		FROM metrics_v5
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY minute_bucket
		ORDER BY minute_bucket ASC
	`,
		bucket,
		avgLatencyExpr,
		p95LatencyExpr,
		ColTeamID,
		ColTimestamp,
		dbLatencyMetricFilter(),
	)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	results := make([]DatabaseAvgLatency, len(rows))
	for i, row := range rows {
		results[i] = DatabaseAvgLatency{
			Timestamp:    dbutil.StringFromAny(row["minute_bucket"]),
			AvgLatencyMs: dbutil.Float64FromAny(row["avg_latency_ms"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
		}
	}
	return results, nil
}

// GetDatabaseCacheSummary returns a single-row rollup of DB query latency,
// total span count, cache hit/miss counts, and average replication lag for the
// requested time window.
func (r *ClickHouseRepository) GetDatabaseCacheSummary(teamUUID string, startMs, endMs int64) (DbCacheSummary, error) {
	avgLatencyExpr := syncAggregateExpr(mongoLatencyAvg(ColAvg), hikariLatencyAvg(ColAvg))
	p95LatencyExpr := syncAggregateExpr(mongoLatencyMax(ColP95), hikariLatencyMax(ColP95))
	queryCountExpr := syncAggregateExpr(
		fmt.Sprintf(`if(maxIf(%s, %s = '%s') > 0, toFloat64(maxIf(%s, %s = '%s')), NULL)`,
			ColCount, ColMetricName, MetricMongoDBDriverCommands,
			ColCount, ColMetricName, MetricMongoDBDriverCommands),
		fmt.Sprintf(`if(maxIf(%s, %s = '%s') > 0, toFloat64(maxIf(%s, %s = '%s')), NULL)`,
			ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
			ColCount, ColMetricName, MetricHikariCPConnectionsUsage),
	)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                        AS avg_query_latency_ms,
		    %s                                                        AS p95_query_latency_ms,
		    toInt64(coalesce(%s, 0))                                  AS db_span_count,
		    sumIf(%s, %s = '%s' AND isFinite(%s))                     AS cache_hits,
		    sumIf(%s, %s = '%s' AND isFinite(%s))                     AS cache_misses,
		    avgIf(%s, %s = '%s' AND isFinite(%s))                     AS avg_replication_lag_ms
		FROM metrics_v5
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s
	`,
		avgLatencyExpr,
		p95LatencyExpr,
		queryCountExpr,
		ColValue, ColMetricName, MetricCacheHits, ColValue,
		ColValue, ColMetricName, MetricCacheMisses, ColValue,
		ColValue, ColMetricName, MetricDBReplicationLagMs, ColValue,
		ColTeamID,
		ColTimestamp,
		dbAllMetricFilter(),
	)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return DbCacheSummary{}, err
	}

	out := DbCacheSummary{
		AvgQueryLatencyMs:   nullableFloat64FromAny(row["avg_query_latency_ms"]),
		P95QueryLatencyMs:   nullableFloat64FromAny(row["p95_query_latency_ms"]),
		DbSpanCount:         dbutil.Int64FromAny(row["db_span_count"]),
		CacheHits:           dbutil.Int64FromAny(row["cache_hits"]),
		CacheMisses:         dbutil.Int64FromAny(row["cache_misses"]),
		AvgReplicationLagMs: nullableFloat64FromAny(row["avg_replication_lag_ms"]),
	}
	return out, nil
}

// GetDatabaseSystems returns a per-DB-system rollup of query counts, latency,
// error counts, and span counts for the requested window. Useful for the
// "Database systems in use" breakdown chart.
func (r *ClickHouseRepository) GetDatabaseSystems(teamUUID string, startMs, endMs int64) ([]DbSystemBreakdown, error) {
	sysExpr := dbSystemExpression()

	query := fmt.Sprintf(`
		SELECT
		    %s                                                          AS db_system,
		    maxIf(%s, %s = '%s')                                        AS mongo_query_count,
		    maxIf(%s, %s = '%s')                                        AS usage_query_count,
		    %s                                                          AS mongo_avg_query_latency_ms,
		    %s                                                          AS usage_avg_query_latency_ms,
		    %s                                                          AS mongo_p95_query_latency_ms,
		    %s                                                          AS usage_p95_query_latency_ms,
		    sumIf(%s, %s = '%s')                                        AS error_count,
		    maxIf(%s, %s = '%s')                                        AS mongo_span_count,
		    maxIf(%s, %s = '%s')                                        AS usage_span_count
		FROM metrics_v5
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY db_system
		ORDER BY greatest(ifNull(mongo_query_count, 0), ifNull(usage_query_count, 0)) DESC
	`,
		sysExpr,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		mongoLatencyAvg(ColAvg),
		hikariLatencyAvg(ColAvg),
		mongoLatencyMax(ColP95),
		hikariLatencyMax(ColP95),
		ColValue, ColMetricName, MetricDBClientErrors,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		ColTeamID,
		ColTimestamp,
		dbAllMetricFilter(),
	)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	breakdown := make([]DbSystemBreakdown, len(rows))
	for i, row := range rows {
		mongoAvg := dbutil.NullableFloat64FromAny(row["mongo_avg_query_latency_ms"])
		usageAvg := dbutil.NullableFloat64FromAny(row["usage_avg_query_latency_ms"])
		mongoP95 := dbutil.NullableFloat64FromAny(row["mongo_p95_query_latency_ms"])
		usageP95 := dbutil.NullableFloat64FromAny(row["usage_p95_query_latency_ms"])

		breakdown[i] = DbSystemBreakdown{
			DbSystem:        dbutil.StringFromAny(row["db_system"]),
			QueryCount:      mergeQueryCount(dbutil.Int64FromAny(row["mongo_query_count"]), dbutil.Int64FromAny(row["usage_query_count"])),
			AvgLatency:      nullableMergedFloatPair(mongoAvg, usageAvg),
			P95QueryLatency: nullableMergedFloatPair(mongoP95, usageP95),
		}
	}
	return breakdown, nil
}

// GetDatabaseTopTables returns the top 50 collections/tables ranked by average
// query latency, enriched with per-system identification, cache hit/miss counts,
// and total query counts. Covers both MongoDB and JDBC/HikariCP sources.
func (r *ClickHouseRepository) GetDatabaseTopTables(teamUUID string, startMs, endMs int64) ([]DbTableMetric, error) {
	collExpr := dbCollectionExpression()
	sysExpr := dbSystemExpression()

	query := fmt.Sprintf(`
		SELECT
		    %s                                                              AS table_name,
		    %s                                                              AS service_name,
		    %s                                                              AS db_system,
		    %s                                                              AS mongo_avg_query_latency_ms,
		    %s                                                              AS usage_avg_query_latency_ms,
		    %s                                                              AS mongo_max_query_latency_ms,
		    %s                                                              AS usage_max_query_latency_ms,
		    sumIf(%s, %s = '%s' AND isFinite(%s))                           AS cache_hits,
		    sumIf(%s, %s = '%s' AND isFinite(%s))                           AS cache_misses,
		    maxIf(%s, %s = '%s')                                            AS mongo_query_count,
		    maxIf(%s, %s = '%s')                                            AS usage_query_count
		FROM metrics_v5
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY table_name, %s, db_system
		ORDER BY greatest(ifNull(mongo_avg_query_latency_ms, 0), ifNull(usage_avg_query_latency_ms, 0)) DESC
		LIMIT %d
	`,
		collExpr,
		ColServiceName,
		sysExpr,
		mongoLatencyAvg(ColAvg),
		hikariLatencyAvg(ColAvg),
		mongoLatencyMax(ColMax),
		hikariLatencyMax(ColMax),
		ColValue, ColMetricName, MetricCacheHits, ColValue,
		ColValue, ColMetricName, MetricCacheMisses, ColValue,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		ColTeamID,
		ColTimestamp,
		dbAllMetricFilter(),
		ColServiceName,
		MaxTopTables,
	)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	metrics := make([]DbTableMetric, len(rows))
	for i, row := range rows {
		mongoAvg := dbutil.NullableFloat64FromAny(row["mongo_avg_query_latency_ms"])
		usageAvg := dbutil.NullableFloat64FromAny(row["usage_avg_query_latency_ms"])
		mongoMax := dbutil.NullableFloat64FromAny(row["mongo_max_query_latency_ms"])
		usageMax := dbutil.NullableFloat64FromAny(row["usage_max_query_latency_ms"])

		metrics[i] = DbTableMetric{
			TableName:         dbutil.StringFromAny(row["table_name"]),
			SystemName:        dbutil.StringFromAny(row["db_system"]),
			DatabaseName:      dbutil.StringFromAny(row["database_name"]),
			AvgQueryLatencyMs: nullableMergedFloatPair(mongoAvg, usageAvg),
			MaxQueryLatencyMs: nullableMergedFloatPair(mongoMax, usageMax),
			CacheHits:         dbutil.Int64FromAny(row["cache_hits"]),
			CacheMisses:       dbutil.Int64FromAny(row["cache_misses"]),
			QueryCount:        mergeQueryCount(dbutil.Int64FromAny(row["mongo_query_count"]), dbutil.Int64FromAny(row["usage_query_count"])),
		}
	}
	return metrics, nil
}
