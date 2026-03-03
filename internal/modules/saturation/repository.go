package saturation

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// queueNameExpression returns a ClickHouse expression to extract queue name from attributes
func queueNameExpression() string {
	return fmt.Sprintf(`coalesce(
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	nullIf(JSONExtractString(%s, '%s'), ''),
	'%s'
)`,
		ColAttributes, AttrMessagingDestinationName,
		ColAttributes, AttrMessagingSourceName,
		ColAttributes, AttrMessagingDestination,
		ColAttributes, AttrMessagingKafkaDestination,
		ColAttributes, AttrMessagingKafkaTopic,
		ColAttributes, AttrKafkaTopic,
		ColAttributes, AttrTopic,
		ColAttributes, AttrQueueName,
		ColAttributes, AttrName,
		DefaultUnknown)
}

// dbCollectionExpression returns a ClickHouse expression to extract database collection/table name
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

// dbSystemExpression returns a ClickHouse expression to determine database system
func dbSystemExpression() string {
	return fmt.Sprintf(`coalesce(
	nullIf(JSONExtractString(%s, '%s'), ''),
	if(
		lower(%s) LIKE '%%mongo%%' OR lower(%s) LIKE '%%mongo%%',
		'%s',
		if(
			lower(%s) LIKE 'hikaricp.%%' OR lower(%s) LIKE 'jdbc.%%'
			OR lower(%s) LIKE '%%mysql%%' OR lower(%s) LIKE '%%postgres%%',
			'%s',
			if(
				lower(%s) LIKE '%%redis%%' OR lower(%s) LIKE '%%redis%%',
				'%s',
				if(
					lower(%s) LIKE '%%sql%%',
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

// dbLatencyMetricFilter returns a filter for database latency metrics
func dbLatencyMetricFilter() string {
	return fmt.Sprintf(`%s IN (%s)`, ColMetricName, MetricSetToInClause(DatabaseLatencyMetrics))
}

// dbAllMetricFilter returns a filter for all database-related metrics
func dbAllMetricFilter() string {
	return fmt.Sprintf(`%s IN (%s)`, ColMetricName, MetricSetToInClause(DatabaseAllMetrics))
}

func syncAggregateExpr(parts ...string) string {
	joined := strings.Join(parts, ", ")
	return `if(
		length(arrayFilter(x -> isNotNull(x), [` + joined + `])) > 0,
		arrayReduce('avg', arrayFilter(x -> isNotNull(x), [` + joined + `])),
		NULL
	)`
}

func mergeNullableFloatPair(a, b *float64) float64 {
	if a != nil && b != nil {
		return (*a + *b) / 2
	}
	if a != nil {
		return *a
	}
	if b != nil {
		return *b
	}
	return 0
}

func nullableMergedFloatPair(a, b *float64) *float64 {
	if a == nil && b == nil {
		return nil
	}
	v := mergeNullableFloatPair(a, b)
	return &v
}

// ClickHouseRepository encapsulates saturation data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new saturation repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetKafkaQueueLag(teamUUID string, startMs, endMs int64) ([]KafkaQueueLag, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	queueExpr := queueNameExpression()

	kafkaLagMetrics := MetricSetToInClause(KafkaConsumerLagMetrics)

	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as queue,
		       %s as minute_bucket,
		       avgIf(%s, %s IN (%s) AND isFinite(%s)) as avg_consumer_lag,
		       maxIf(%s, %s IN (%s) AND isFinite(%s)) as max_consumer_lag
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, queueExpr, bucket,
		ColValue, ColMetricName, kafkaLagMetrics, ColValue,
		ColValue, ColMetricName, kafkaLagMetrics, ColValue,
		ColTeamID, ColTimestamp,
		ColMetricName, kafkaLagMetrics),
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	results := make([]KafkaQueueLag, len(rows))
	for i, row := range rows {
		results[i] = KafkaQueueLag{
			Queue:          dbutil.StringFromAny(row["queue"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgConsumerLag: dbutil.Float64FromAny(row["avg_consumer_lag"]),
			MaxConsumerLag: dbutil.Float64FromAny(row["max_consumer_lag"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetKafkaProductionRate(teamUUID string, startMs, endMs int64) ([]KafkaProductionRate, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	bucketSeconds := TimeBucketSeconds(startMs, endMs)
	queueExpr := queueNameExpression()
	producerMetrics := MetricSetToInClause(KafkaProducerMetrics)

	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as queue,
		       %s as minute_bucket,
		       maxIf(toFloat64(%s), %s IN (%s) AND isFinite(%s)) / ? as avg_publish_rate
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, queueExpr, bucket,
		ColValue, ColMetricName, producerMetrics, ColValue,
		ColTeamID, ColTimestamp,
		ColMetricName, producerMetrics),
		bucketSeconds, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	results := make([]KafkaProductionRate, len(rows))
	for i, row := range rows {
		results[i] = KafkaProductionRate{
			Queue:          dbutil.StringFromAny(row["queue"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgPublishRate: dbutil.Float64FromAny(row["avg_publish_rate"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetKafkaConsumptionRate(teamUUID string, startMs, endMs int64) ([]KafkaConsumptionRate, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	bucketSeconds := TimeBucketSeconds(startMs, endMs)
	queueExpr := queueNameExpression()
	consumerMetrics := MetricSetToInClause(KafkaConsumerMetrics)

	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as queue,
		       %s as minute_bucket,
		       maxIf(toFloat64(%s), %s IN (%s) AND isFinite(%s)) / ? as avg_receive_rate
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, queueExpr, bucket,
		ColValue, ColMetricName, consumerMetrics, ColValue,
		ColTeamID, ColTimestamp,
		ColMetricName, consumerMetrics),
		bucketSeconds, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	results := make([]KafkaConsumptionRate, len(rows))
	for i, row := range rows {
		results[i] = KafkaConsumptionRate{
			Queue:          dbutil.StringFromAny(row["queue"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgReceiveRate: dbutil.Float64FromAny(row["avg_receive_rate"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetDatabaseQueryByTable(teamUUID string, startMs, endMs int64) ([]DatabaseQueryByTable, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	dbCollExpr := dbCollectionExpression()

	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as table_name,
		       %s as minute_bucket,
		       maxIf(%s, %s = '%s') as mongo_query_count,
		       maxIf(%s, %s = '%s') as usage_query_count,
		       avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as mongo_avg_latency_ms,
		       avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as usage_avg_latency_ms,
		       maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as mongo_p95_latency_ms,
		       maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as usage_p95_latency_ms
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY table_name, minute_bucket
		ORDER BY minute_bucket ASC, table_name ASC
	`, dbCollExpr, bucket,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		ColAvg, ColMetricName, MetricMongoDBDriverCommands, ColAvg,
		ColAvg, ColMetricName, MetricHikariCPConnectionsUsage, ColAvg,
		ColP95, ColMetricName, MetricMongoDBDriverCommands, ColP95,
		ColP95, ColMetricName, MetricHikariCPConnectionsUsage, ColP95,
		ColTeamID, ColTimestamp,
		dbLatencyMetricFilter()), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	results := make([]DatabaseQueryByTable, len(rows))
	for i, row := range rows {
		mongoAvg := dbutil.NullableFloat64FromAny(row["mongo_avg_latency_ms"])
		usageAvg := dbutil.NullableFloat64FromAny(row["usage_avg_latency_ms"])
		mongoP95 := dbutil.NullableFloat64FromAny(row["mongo_p95_latency_ms"])
		usageP95 := dbutil.NullableFloat64FromAny(row["usage_p95_latency_ms"])
		queryCount := dbutil.Int64FromAny(row["mongo_query_count"])
		if usageCount := dbutil.Int64FromAny(row["usage_query_count"]); usageCount > 0 {
			if queryCount > 0 {
				queryCount = (queryCount + usageCount) / 2
			} else {
				queryCount = usageCount
			}
		}
		results[i] = DatabaseQueryByTable{
			Table:        dbutil.StringFromAny(row["table_name"]),
			Timestamp:    dbutil.StringFromAny(row["minute_bucket"]),
			QueryCount:   queryCount,
			AvgLatencyMs: mergeNullableFloatPair(mongoAvg, usageAvg),
			P95LatencyMs: mergeNullableFloatPair(mongoP95, usageP95),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetDatabaseAvgLatency(teamUUID string, startMs, endMs int64) ([]DatabaseAvgLatency, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	avgLatencyExpr := syncAggregateExpr(
		fmt.Sprintf(`avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColAvg, ColMetricName, MetricMongoDBDriverCommands, ColAvg),
		fmt.Sprintf(`avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColAvg, ColMetricName, MetricHikariCPConnectionsUsage, ColAvg),
	)
	p95LatencyExpr := syncAggregateExpr(
		fmt.Sprintf(`maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColP95, ColMetricName, MetricMongoDBDriverCommands, ColP95),
		fmt.Sprintf(`maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColP95, ColMetricName, MetricHikariCPConnectionsUsage, ColP95),
	)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as minute_bucket,
		       coalesce(%s, 0) as avg_latency_ms,
		       coalesce(%s, 0) as p95_latency_ms
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY minute_bucket
		ORDER BY minute_bucket ASC
	`, bucket, avgLatencyExpr, p95LatencyExpr, ColTeamID, ColTimestamp, dbLatencyMetricFilter()), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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

// GetDatabaseCacheSummary queries DB query latency and cache-hit insights from metrics.
func (r *ClickHouseRepository) GetDatabaseCacheSummary(teamUUID string, startMs, endMs int64) (DbCacheSummary, error) {
	avgLatencyExpr := syncAggregateExpr(
		fmt.Sprintf(`avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColAvg, ColMetricName, MetricMongoDBDriverCommands, ColAvg),
		fmt.Sprintf(`avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColAvg, ColMetricName, MetricHikariCPConnectionsUsage, ColAvg),
	)
	p95LatencyExpr := syncAggregateExpr(
		fmt.Sprintf(`maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColP95, ColMetricName, MetricMongoDBDriverCommands, ColP95),
		fmt.Sprintf(`maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000`, ColP95, ColMetricName, MetricHikariCPConnectionsUsage, ColP95),
	)
	queryCountExpr := syncAggregateExpr(
		fmt.Sprintf(`if(maxIf(%s, %s = '%s') > 0, toFloat64(maxIf(%s, %s = '%s')), NULL)`, ColCount, ColMetricName, MetricMongoDBDriverCommands, ColCount, ColMetricName, MetricMongoDBDriverCommands),
		fmt.Sprintf(`if(maxIf(%s, %s = '%s') > 0, toFloat64(maxIf(%s, %s = '%s')), NULL)`, ColCount, ColMetricName, MetricHikariCPConnectionsUsage, ColCount, ColMetricName, MetricHikariCPConnectionsUsage),
	)
	summaryRaw, err := dbutil.QueryMap(r.db, fmt.Sprintf(`
		SELECT %s as avg_query_latency_ms,
		       %s as p95_query_latency_ms,
		       toInt64(coalesce(%s, 0)) as db_span_count,
		       sumIf(%s, %s = '%s' AND isFinite(%s)) as cache_hits,
		       sumIf(%s, %s = '%s' AND isFinite(%s)) as cache_misses,
		       avgIf(%s, %s = '%s' AND isFinite(%s)) as avg_replication_lag_ms
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s
	`, avgLatencyExpr, p95LatencyExpr, queryCountExpr,
		ColValue, ColMetricName, MetricCacheHits, ColValue,
		ColValue, ColMetricName, MetricCacheMisses, ColValue,
		ColValue, ColMetricName, MetricDBReplicationLagMs, ColValue,
		ColTeamID, ColTimestamp,
		dbAllMetricFilter()), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return DbCacheSummary{}, err
	}

	return DbCacheSummary{
		AvgQueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["avg_query_latency_ms"]),
		P95QueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["p95_query_latency_ms"]),
		DbSpanCount:         dbutil.Int64FromAny(summaryRaw["db_span_count"]),
		CacheHits:           dbutil.Int64FromAny(summaryRaw["cache_hits"]),
		CacheMisses:         dbutil.Int64FromAny(summaryRaw["cache_misses"]),
		AvgReplicationLagMs: dbutil.NullableFloat64FromAny(summaryRaw["avg_replication_lag_ms"]),
	}, nil
}

// GetDatabaseSystems queries DB metrics grouped by the database system.
func (r *ClickHouseRepository) GetDatabaseSystems(teamUUID string, startMs, endMs int64) ([]DbSystemBreakdown, error) {
	dbSysExpr := dbSystemExpression()

	systemRaw, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as db_system,
		       maxIf(%s, %s = '%s') as mongo_query_count,
		       maxIf(%s, %s = '%s') as usage_query_count,
		       avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as mongo_avg_query_latency_ms,
		       avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as usage_avg_query_latency_ms,
		       maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as mongo_p95_query_latency_ms,
		       maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as usage_p95_query_latency_ms,
		       sumIf(%s, %s = '%s') as error_count,
		       maxIf(%s, %s = '%s') as mongo_span_count,
		       maxIf(%s, %s = '%s') as usage_span_count
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY db_system
		ORDER BY greatest(ifNull(mongo_query_count, 0), ifNull(usage_query_count, 0)) DESC
	`, dbSysExpr,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		ColAvg, ColMetricName, MetricMongoDBDriverCommands, ColAvg,
		ColAvg, ColMetricName, MetricHikariCPConnectionsUsage, ColAvg,
		ColP95, ColMetricName, MetricMongoDBDriverCommands, ColP95,
		ColP95, ColMetricName, MetricHikariCPConnectionsUsage, ColP95,
		ColValue, ColMetricName, MetricDBClientErrors,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		ColTeamID, ColTimestamp,
		dbAllMetricFilter()), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	systemBreakdown := make([]DbSystemBreakdown, len(systemRaw))
	for i, row := range systemRaw {
		mongoAvg := dbutil.NullableFloat64FromAny(row["mongo_avg_query_latency_ms"])
		usageAvg := dbutil.NullableFloat64FromAny(row["usage_avg_query_latency_ms"])
		mongoP95 := dbutil.NullableFloat64FromAny(row["mongo_p95_query_latency_ms"])
		usageP95 := dbutil.NullableFloat64FromAny(row["usage_p95_query_latency_ms"])
		queryCount := dbutil.Int64FromAny(row["mongo_query_count"])
		if usageCount := dbutil.Int64FromAny(row["usage_query_count"]); usageCount > 0 {
			if queryCount > 0 {
				queryCount = (queryCount + usageCount) / 2
			} else {
				queryCount = usageCount
			}
		}
		spanCount := dbutil.Int64FromAny(row["mongo_span_count"])
		if usageSpanCount := dbutil.Int64FromAny(row["usage_span_count"]); usageSpanCount > 0 {
			if spanCount > 0 {
				spanCount = (spanCount + usageSpanCount) / 2
			} else {
				spanCount = usageSpanCount
			}
		}
		systemBreakdown[i] = DbSystemBreakdown{
			DbSystem:        dbutil.StringFromAny(row["db_system"]),
			QueryCount:      queryCount,
			AvgQueryLatency: nullableMergedFloatPair(mongoAvg, usageAvg),
			P95QueryLatency: nullableMergedFloatPair(mongoP95, usageP95),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			SpanCount:       spanCount,
		}
	}

	return systemBreakdown, nil
}

// GetDatabaseTopTables queries table-specific DB latency, hits, and misses.
func (r *ClickHouseRepository) GetDatabaseTopTables(teamUUID string, startMs, endMs int64) ([]DbTableMetric, error) {
	dbCollExpr := dbCollectionExpression()
	dbSysExpr := dbSystemExpression()

	tableMetricsRaw, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as table_name,
		       %s,
		       %s as db_system,
		       avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as mongo_avg_query_latency_ms,
		       avgIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as usage_avg_query_latency_ms,
		       maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as mongo_max_query_latency_ms,
		       maxIf(%s, %s = '%s' AND isFinite(%s)) * 1000 as usage_max_query_latency_ms,
		       sumIf(%s, %s = '%s' AND isFinite(%s)) as cache_hits,
		       sumIf(%s, %s = '%s' AND isFinite(%s)) as cache_misses,
		       maxIf(%s, %s = '%s') as mongo_query_count,
		       maxIf(%s, %s = '%s') as usage_query_count
		FROM metrics
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s
		GROUP BY table_name, %s, db_system
		ORDER BY greatest(ifNull(mongo_avg_query_latency_ms, 0), ifNull(usage_avg_query_latency_ms, 0)) DESC
		LIMIT 50
	`, dbCollExpr,
		ColServiceName,
		dbSysExpr,
		ColAvg, ColMetricName, MetricMongoDBDriverCommands, ColAvg,
		ColAvg, ColMetricName, MetricHikariCPConnectionsUsage, ColAvg,
		ColMax, ColMetricName, MetricMongoDBDriverCommands, ColMax,
		ColMax, ColMetricName, MetricHikariCPConnectionsUsage, ColMax,
		ColValue, ColMetricName, MetricCacheHits, ColValue,
		ColValue, ColMetricName, MetricCacheMisses, ColValue,
		ColCount, ColMetricName, MetricMongoDBDriverCommands,
		ColCount, ColMetricName, MetricHikariCPConnectionsUsage,
		ColTeamID, ColTimestamp,
		dbAllMetricFilter(),
		ColServiceName), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	tableMetrics := make([]DbTableMetric, len(tableMetricsRaw))
	for i, row := range tableMetricsRaw {
		mongoAvg := dbutil.NullableFloat64FromAny(row["mongo_avg_query_latency_ms"])
		usageAvg := dbutil.NullableFloat64FromAny(row["usage_avg_query_latency_ms"])
		mongoMax := dbutil.NullableFloat64FromAny(row["mongo_max_query_latency_ms"])
		usageMax := dbutil.NullableFloat64FromAny(row["usage_max_query_latency_ms"])
		queryCount := dbutil.Int64FromAny(row["mongo_query_count"])
		if usageCount := dbutil.Int64FromAny(row["usage_query_count"]); usageCount > 0 {
			if queryCount > 0 {
				queryCount = (queryCount + usageCount) / 2
			} else {
				queryCount = usageCount
			}
		}
		tableMetrics[i] = DbTableMetric{
			TableName:         dbutil.StringFromAny(row["table_name"]),
			ServiceName:       dbutil.StringFromAny(row["service_name"]),
			DbSystem:          dbutil.StringFromAny(row["db_system"]),
			AvgQueryLatencyMs: nullableMergedFloatPair(mongoAvg, usageAvg),
			MaxQueryLatencyMs: nullableMergedFloatPair(mongoMax, usageMax),
			CacheHits:         dbutil.Int64FromAny(row["cache_hits"]),
			CacheMisses:       dbutil.Int64FromAny(row["cache_misses"]),
			QueryCount:        queryCount,
		}
	}

	return tableMetrics, nil
}

// GetQueueConsumerLag queries the consumer lag timeseries for queues.
func (r *ClickHouseRepository) GetQueueConsumerLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	bucket := FormattedTimeBucketExpression(startMs, endMs)
	consumerLagMetrics := MetricSetToInClause(KafkaConsumerLagMetricsExtended)

	timeseriesRaw, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as time_bucket,
		       if(service_name != '', service_name, 'unknown') as service_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.source.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'topic'), ''),
		           nullIf(JSONExtractString(attributes, 'queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'name'), ''),
		           nullIf(if(metric_name IN (%s), 'kafka-consumer-lag', ''), ''),
		           'unknown'
		       ) as queue_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.system'), ''),
		           if(metric_name LIKE 'kafka.%%' OR metric_name LIKE 'messaging.kafka.%%', 'kafka', ''),
		           'kafka'
		       ) as messaging_system,
		       avgIf(value, metric_name IN (%s) AND isFinite(value)) as avg_consumer_lag
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (%s)
		GROUP BY time_bucket, service_name, queue_name, messaging_system
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC
	`, bucket, consumerLagMetrics, consumerLagMetrics, consumerLagMetrics), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	timeseries := make([]MqBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = MqBucket{
			Timestamp:       dbutil.StringFromAny(row["time_bucket"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgConsumerLag:  dbutil.NullableFloat64FromAny(row["avg_consumer_lag"]),
		}
	}
	return timeseries, nil
}

// GetQueueTopicLag queries the queue depth timeseries for topics.
func (r *ClickHouseRepository) GetQueueTopicLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	bucket := FormattedTimeBucketExpression(startMs, endMs)
	queueDepthMetrics := MetricSetToInClause(QueueDepthMetrics)

	timeseriesRaw, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as time_bucket,
		       if(service_name != '', service_name, 'unknown') as service_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'topic'), ''),
		           nullIf(JSONExtractString(attributes, 'queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'name'), ''),
		           'unknown'
		       ) as queue_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.system'), ''),
		           if(metric_name LIKE 'kafka.%%' OR metric_name LIKE 'messaging.kafka.%%', 'kafka', ''),
		           'kafka'
		       ) as messaging_system,
		       avgIf(value, metric_name IN (%s) AND isFinite(value)) as avg_queue_depth
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (%s)
		GROUP BY time_bucket, service_name, queue_name, messaging_system
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC
	`, bucket, queueDepthMetrics, queueDepthMetrics), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	timeseries := make([]MqBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = MqBucket{
			Timestamp:       dbutil.StringFromAny(row["time_bucket"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgQueueDepth:   dbutil.NullableFloat64FromAny(row["avg_queue_depth"]),
		}
	}
	return timeseries, nil
}

// GetQueueTopQueues gets the summarized top queues across messaging systems along with rates.
func (r *ClickHouseRepository) GetQueueTopQueues(teamUUID string, startMs, endMs int64) ([]MqTopQueue, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}

	allQueueMetrics := MetricSetToInClause(AllQueueMetrics)
	consumerLagMetrics := MetricSetToInClause(KafkaConsumerLagMetricsExtended)
	queueDepthMetrics := MetricSetToInClause(QueueDepthMetrics)

	topQueuesRaw, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.source.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'topic'), ''),
		           nullIf(JSONExtractString(attributes, 'queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'name'), ''),
		           nullIf(if(metric_name IN (%s), 'kafka-consumer-lag', ''), ''),
		           'unknown'
		       ) as queue_name,
		       if(service_name != '', service_name, 'unknown') as service_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.system'), ''),
		           if(metric_name LIKE 'kafka.%%' OR metric_name LIKE 'messaging.kafka.%%', 'kafka', ''),
		           'kafka'
		       ) as messaging_system,
		       avgIf(value, metric_name IN (%s) AND isFinite(value)) as avg_queue_depth,
		       maxIf(value, metric_name IN (%s) AND isFinite(value)) as max_consumer_lag,
		       maxIf(toFloat64(count), metric_name = '%s') / ? as avg_publish_rate,
		       maxIf(toFloat64(count), metric_name = '%s') / ? as avg_receive_rate,
		       toInt64(count()) as sample_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (%s)
		GROUP BY queue_name, service_name, messaging_system
		ORDER BY avg_queue_depth DESC, max_consumer_lag DESC, sample_count DESC
		LIMIT 50
	`, consumerLagMetrics, queueDepthMetrics, consumerLagMetrics,
		MetricSpringKafkaTemplate, MetricSpringKafkaListener, allQueueMetrics),
		durationSecs, durationSecs, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	topQueues := make([]MqTopQueue, len(topQueuesRaw))
	for i, row := range topQueuesRaw {
		topQueues[i] = MqTopQueue{
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgQueueDepth:   dbutil.NullableFloat64FromAny(row["avg_queue_depth"]),
			MaxConsumerLag:  dbutil.NullableFloat64FromAny(row["max_consumer_lag"]),
			AvgPublishRate:  dbutil.Float64FromAny(row["avg_publish_rate"]),
			AvgReceiveRate:  dbutil.Float64FromAny(row["avg_receive_rate"]),
			SampleCount:     dbutil.Int64FromAny(row["sample_count"]),
		}
	}

	return topQueues, nil
}
