package database

import (
	"fmt"
	"math"

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
	nullIf(%s, ''),
	nullIf(%s, ''),
	nullIf(%s, ''),
	nullIf(%s, ''),
	nullIf(%s, ''),
	nullIf(%s, ''),
	'%s'
)`,
		attrString(AttrDBMongoDBCollection),
		attrString(AttrCollection),
		attrString(AttrDBCollectionName),
		attrString(AttrDBSQLTable),
		attrString(AttrPool),
		attrString(AttrDBStatement),
		DefaultUnknown)
}

// dbSystemExpression resolves the database engine (mongodb, mysql, redis, sql)
// using, in order: the `db.system` attribute, metric-name pattern matching, and
// service-name pattern matching. The nested ifs mirror ClickHouse's lack of a
// CASE WHEN; they are structured for readability with one branch per DB system.
func dbSystemExpression() string {
	aSystem := attrString(AttrDBSystem)
	return fmt.Sprintf(`coalesce(
	nullIf(%s, ''),
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
		aSystem,
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
	out := 0.0
	switch v := val.(type) {
	case *float64:
		if v != nil {
			out = *v
		}
	case float64:
		out = v
	case *float32:
		if v != nil {
			out = float64(*v)
		}
	case float32:
		out = float64(v)
	}
	if math.IsNaN(out) || math.IsInf(out, 0) {
		return 0
	}
	return out
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
		FROM %s
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
		TableMetrics,
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
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY minute_bucket
		ORDER BY minute_bucket ASC
	`,
		bucket,
		avgLatencyExpr,
		p95LatencyExpr,
		TableMetrics,
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
		FROM %s
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
		TableMetrics,
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
		FROM %s
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
		TableMetrics,
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
		FROM %s
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
		TableMetrics,
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

// ─── db.client.* OTel standard metrics ────────────────────────────────────────

// GetConnectionCount returns connection counts grouped by state over time.
func (r *ClickHouseRepository) GetConnectionCount(teamUUID string, startMs, endMs int64) ([]ConnectionStatValue, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s                                                  AS time_bucket,
		    %s                                                  AS state,
		    avg(value)                                          AS val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state
	`,
		bucket,
		attrString(AttrDBConnectionState),
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBClientConnectionCount,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]ConnectionStatValue, len(rows))
	for i, row := range rows {
		v := dbutil.NullableFloat64FromAny(row["val"])
		results[i] = ConnectionStatValue{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			State:     dbutil.StringFromAny(row["state"]),
			Value:     v,
		}
	}
	return results, nil
}

// GetConnectionWaitTime returns histogram summary (p50/p95/p99/avg) for connection wait time.
func (r *ClickHouseRepository) GetConnectionWaitTime(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99,
		    avg(hist_sum / nullIf(hist_count, 0))                                     AS avg_val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricDBClientConnectionWait,
	)
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return HistogramSummary{}, err
	}
	return HistogramSummary{
		P50: dbutil.Float64FromAny(row["p50"]),
		P95: dbutil.Float64FromAny(row["p95"]),
		P99: dbutil.Float64FromAny(row["p99"]),
		Avg: dbutil.Float64FromAny(row["avg_val"]),
	}, nil
}

// GetConnectionPending returns pending connection request counts over time.
func (r *ClickHouseRepository) GetConnectionPending(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s         AS minute_bucket,
		    avg(value) AS avg_latency_ms,
		    0          AS p95_latency_ms
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY minute_bucket
		ORDER BY minute_bucket
	`,
		bucket, TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricDBClientConnectionPending,
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

// GetConnectionTimeouts returns connection timeout counts over time.
func (r *ClickHouseRepository) GetConnectionTimeouts(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s         AS minute_bucket,
		    sum(value) AS avg_latency_ms,
		    0          AS p95_latency_ms
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY minute_bucket
		ORDER BY minute_bucket
	`,
		bucket, TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricDBClientConnectionTimeouts,
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

// GetQueryDuration returns histogram summary for db.client.operation.duration.
func (r *ClickHouseRepository) GetQueryDuration(teamUUID string, startMs, endMs int64) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99,
		    avg(hist_sum / nullIf(hist_count, 0))                                     AS avg_val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricDBClientOperationDuration,
	)
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return HistogramSummary{}, err
	}
	return HistogramSummary{
		P50: dbutil.Float64FromAny(row["p50"]),
		P95: dbutil.Float64FromAny(row["p95"]),
		P99: dbutil.Float64FromAny(row["p99"]),
		Avg: dbutil.Float64FromAny(row["avg_val"]),
	}, nil
}

// ─── Redis metrics ─────────────────────────────────────────────────────────────

// GetRedisCacheHitRate returns Redis keyspace hit/miss counts and computed hit rate.
func (r *ClickHouseRepository) GetRedisCacheHitRate(teamUUID string, startMs, endMs int64) (RedisHitRate, error) {
	query := fmt.Sprintf(`
		SELECT
		    toInt64(sumIf(value, %s = '%s')) AS hits,
		    toInt64(sumIf(value, %s = '%s')) AS misses
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
	`,
		ColMetricName, MetricRedisKeyspaceHits,
		ColMetricName, MetricRedisKeyspaceMisses,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricRedisKeyspaceHits, MetricRedisKeyspaceMisses,
	)
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return RedisHitRate{}, err
	}
	hits := dbutil.Int64FromAny(row["hits"])
	misses := dbutil.Int64FromAny(row["misses"])
	total := hits + misses
	var rate float64
	if total > 0 {
		rate = float64(hits) / float64(total) * 100.0
	}
	return RedisHitRate{Hits: hits, Misses: misses, HitRatePct: rate}, nil
}

// GetRedisConnectedClients returns connected client count over time.
func (r *ClickHouseRepository) GetRedisConnectedClients(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return r.queryRedisTimeSeries(teamUUID, startMs, endMs, MetricRedisClientsConnected)
}

// GetRedisMemoryUsed returns Redis memory usage over time.
func (r *ClickHouseRepository) GetRedisMemoryUsed(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return r.queryRedisTimeSeries(teamUUID, startMs, endMs, MetricRedisMemoryUsed)
}

// GetRedisMemoryFragmentation returns memory fragmentation ratio over time.
func (r *ClickHouseRepository) GetRedisMemoryFragmentation(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return r.queryRedisTimeSeries(teamUUID, startMs, endMs, MetricRedisMemoryFragmentationRatio)
}

// GetRedisCommandRate returns Redis commands processed per time bucket.
func (r *ClickHouseRepository) GetRedisCommandRate(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return r.queryRedisTimeSeries(teamUUID, startMs, endMs, MetricRedisCommandsProcessed)
}

// GetRedisEvictions returns key eviction count over time.
func (r *ClickHouseRepository) GetRedisEvictions(teamUUID string, startMs, endMs int64) ([]RedisTimeSeries, error) {
	return r.queryRedisTimeSeries(teamUUID, startMs, endMs, MetricRedisKeysEvicted)
}

// GetRedisKeyspaceSize returns per-DB key counts.
func (r *ClickHouseRepository) GetRedisKeyspaceSize(teamUUID string, startMs, endMs int64) ([]RedisDBKeyStat, error) {
	return r.queryRedisDBStat(teamUUID, startMs, endMs, MetricRedisDBKeys)
}

// GetRedisKeyExpiries returns per-DB expiring key counts.
func (r *ClickHouseRepository) GetRedisKeyExpiries(teamUUID string, startMs, endMs int64) ([]RedisDBKeyStat, error) {
	return r.queryRedisDBStat(teamUUID, startMs, endMs, MetricRedisDBExpires)
}

// GetRedisReplicationLag returns replication offset and backlog offset.
func (r *ClickHouseRepository) GetRedisReplicationLag(teamUUID string, startMs, endMs int64) (RedisReplicationLag, error) {
	query := fmt.Sprintf(`
		SELECT
		    toInt64(avgIf(value, %s = '%s')) AS repl_offset,
		    toInt64(avgIf(value, %s = '%s')) AS backlog_offset
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
	`,
		ColMetricName, MetricRedisReplicationOffset,
		ColMetricName, MetricRedisReplicationBacklogOffset,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricRedisReplicationOffset, MetricRedisReplicationBacklogOffset,
	)
	row, err := dbutil.QueryMap(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return RedisReplicationLag{}, err
	}
	return RedisReplicationLag{
		Offset:        dbutil.Int64FromAny(row["repl_offset"]),
		BacklogOffset: dbutil.Int64FromAny(row["backlog_offset"]),
	}, nil
}

// queryRedisTimeSeries is a shared helper for scalar Redis timeseries queries.
func (r *ClickHouseRepository) queryRedisTimeSeries(teamUUID string, startMs, endMs int64, metricName string) ([]RedisTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    avg(value) AS val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket, TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]RedisTimeSeries, len(rows))
	for i, row := range rows {
		v := dbutil.NullableFloat64FromAny(row["val"])
		results[i] = RedisTimeSeries{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Value:     v,
		}
	}
	return results, nil
}

// queryRedisDBStat returns per-DB aggregate stats (sum of value grouped by redis.db attr).
func (r *ClickHouseRepository) queryRedisDBStat(teamUUID string, startMs, endMs int64, metricName string) ([]RedisDBKeyStat, error) {
	query := fmt.Sprintf(`
		SELECT
		    %s              AS db_name,
		    toInt64(sum(value)) AS key_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY db_name
		ORDER BY key_count DESC
	`,
		attrString(AttrRedisDB),
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]RedisDBKeyStat, len(rows))
	for i, row := range rows {
		results[i] = RedisDBKeyStat{
			DB:   dbutil.StringFromAny(row["db_name"]),
			Keys: dbutil.Int64FromAny(row["key_count"]),
		}
	}
	return results, nil
}
