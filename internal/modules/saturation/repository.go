package saturation

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

const queueNameExpr = `coalesce(
	nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
	nullIf(JSONExtractString(attributes, 'messaging.source.name'), ''),
	nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
	nullIf(JSONExtractString(attributes, 'messaging.kafka.destination'), ''),
	nullIf(JSONExtractString(attributes, 'messaging.kafka.topic'), ''),
	nullIf(JSONExtractString(attributes, 'kafka.topic'), ''),
	nullIf(JSONExtractString(attributes, 'topic'), ''),
	nullIf(JSONExtractString(attributes, 'queue.name'), ''),
	nullIf(JSONExtractString(attributes, 'name'), ''),
	'unknown'
)`

const dbCollectionExpr = `coalesce(
	nullIf(JSONExtractString(attributes, 'db.mongodb.collection'), ''),
	nullIf(JSONExtractString(attributes, 'collection'), ''),
	nullIf(JSONExtractString(attributes, 'db.collection.name'), ''),
	nullIf(JSONExtractString(attributes, 'db.sql.table'), ''),
	nullIf(JSONExtractString(attributes, 'pool'), ''),
	nullIf(JSONExtractString(attributes, 'db.statement'), ''),
	'unknown'
)`

const dbSystemExpr = `coalesce(
	nullIf(JSONExtractString(attributes, 'db.system'), ''),
	if(
		lower(metric_name) LIKE '%mongo%' OR lower(service_name) LIKE '%mongo%',
		'mongodb',
		if(
			lower(metric_name) LIKE 'hikaricp.%' OR lower(metric_name) LIKE 'jdbc.%'
			OR lower(service_name) LIKE '%mysql%' OR lower(service_name) LIKE '%postgres%',
			'mysql',
			if(
				lower(metric_name) LIKE '%redis%' OR lower(service_name) LIKE '%redis%',
				'redis',
				if(
					lower(metric_name) LIKE '%sql%',
					'sql',
					'unknown'
				)
			)
		)
	)
)`

const dbLatencyMetricFilter = `metric_name IN (
	'mongodb.driver.commands',
	'hikaricp.connections.usage'
)`

const dbAllMetricFilter = `metric_name IN (
	'mongodb.driver.commands',
	'hikaricp.connections.acquire',
	'hikaricp.connections.usage',
	'hikaricp.connections.active',
	'hikaricp.connections.max',
	'jdbc.connections.active',
	'jdbc.connections.max',
	'cache.hits',
	'cache.misses',
	'db.replication.lag.ms',
	'db.client.errors'
)`

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

// satBucketExpr returns a ClickHouse time-bucketing expression for adaptive granularity.
func satBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "toStartOfMinute(timestamp)"
	case hours <= 24:
		return "toStartOfFiveMinutes(timestamp)"
	case hours <= 168:
		return "toStartOfHour(timestamp)"
	default:
		return "toStartOfDay(timestamp)"
	}
}

// satFmtBucketExpr returns a formatted ClickHouse time-bucketing expression.
func satFmtBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	case hours <= 24:
		return "formatDateTime(toStartOfFiveMinutes(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	case hours <= 168:
		return "formatDateTime(toStartOfHour(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	default:
		return "formatDateTime(toStartOfDay(timestamp), '%Y-%m-%dT%H:%i:%SZ')"
	}
}

func satBucketSeconds(startMs, endMs int64) float64 {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return 60
	case hours <= 24:
		return 300
	case hours <= 168:
		return 3600
	default:
		return 86400
	}
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
	bucket := satBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as queue,
		       %s as minute_bucket,
		       avgIf(value, metric_name IN (
		           'messaging.kafka.consumer.lag',
		           'messaging.kafka.consumer.records.lag',
		           'messaging.kafka.consumer.records-lag',
		           'messaging.kafka.consumer.records.lag.max',
		           'kafka.consumer.lag',
		           'kafka.consumer.records.lag',
		           'kafka.consumer.records-lag',
		           'kafka.consumer.records.lag.max',
		           'kafka.consumer.fetch.manager.records.lag',
		           'kafka.consumer.fetch.manager.records.lag.max',
		           'kafka.consumer.fetch.records.lag.max'
		       ) AND isFinite(value)) as avg_consumer_lag,
		       maxIf(value, metric_name IN (
		           'messaging.kafka.consumer.lag',
		           'messaging.kafka.consumer.records.lag',
		           'messaging.kafka.consumer.records-lag',
		           'messaging.kafka.consumer.records.lag.max',
		           'kafka.consumer.lag',
		           'kafka.consumer.records.lag',
		           'kafka.consumer.records-lag',
		           'kafka.consumer.records.lag.max',
		           'kafka.consumer.fetch.manager.records.lag',
		           'kafka.consumer.fetch.manager.records.lag.max',
		           'kafka.consumer.fetch.records.lag.max'
		       ) AND isFinite(value)) as max_consumer_lag
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'messaging.kafka.consumer.lag',
		      'messaging.kafka.consumer.records.lag',
		      'messaging.kafka.consumer.records-lag',
		      'messaging.kafka.consumer.records.lag.max',
		      'kafka.consumer.lag',
		      'kafka.consumer.records.lag',
		      'kafka.consumer.records-lag',
		      'kafka.consumer.records.lag.max',
		      'kafka.consumer.fetch.manager.records.lag',
		      'kafka.consumer.fetch.manager.records.lag.max',
		      'kafka.consumer.fetch.records.lag.max'
		  )
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, queueNameExpr, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	bucket := satBucketExpr(startMs, endMs)
	bucketSeconds := satBucketSeconds(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as queue,
		       %s as minute_bucket,
		       maxIf(
		           toFloat64(value),
		           metric_name IN (
		               'kafka.producer.record.send.total',
		               'spring.kafka.template'
		           ) AND isFinite(value)
		       ) / ? as avg_publish_rate
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'spring.kafka.template',
		      'kafka.producer.record.send.total'
		  )
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, queueNameExpr, bucket), bucketSeconds, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	bucket := satBucketExpr(startMs, endMs)
	bucketSeconds := satBucketSeconds(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as queue,
		       %s as minute_bucket,
		       maxIf(
		           toFloat64(value),
		           metric_name IN (
		               'spring.kafka.listener',
		               'kafka.consumer.fetch.manager.records.consumed.total'
		           ) AND isFinite(value)
		       ) / ? as avg_receive_rate
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'spring.kafka.listener',
		      'kafka.consumer.fetch.manager.records.consumed.total'
		  )
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, queueNameExpr, bucket), bucketSeconds, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	bucket := satBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as table_name,
		       %s as minute_bucket,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as mongo_query_count,
		       maxIf(count, metric_name = 'hikaricp.connections.usage') as usage_query_count,
		       avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as mongo_avg_latency_ms,
		       avgIf(avg, metric_name = 'hikaricp.connections.usage' AND isFinite(avg)) * 1000 as usage_avg_latency_ms,
		       maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000 as mongo_p95_latency_ms,
		       maxIf(p95, metric_name = 'hikaricp.connections.usage' AND isFinite(p95)) * 1000 as usage_p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND `+dbLatencyMetricFilter+`
		GROUP BY table_name, minute_bucket
		ORDER BY minute_bucket ASC, table_name ASC
	`, dbCollectionExpr, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	bucket := satBucketExpr(startMs, endMs)
	avgLatencyExpr := syncAggregateExpr(
		`avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000`,
		`avgIf(avg, metric_name = 'hikaricp.connections.usage' AND isFinite(avg)) * 1000`,
	)
	p95LatencyExpr := syncAggregateExpr(
		`maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000`,
		`maxIf(p95, metric_name = 'hikaricp.connections.usage' AND isFinite(p95)) * 1000`,
	)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as minute_bucket,
		       coalesce(%s, 0) as avg_latency_ms,
		       coalesce(%s, 0) as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND `+dbLatencyMetricFilter+`
		GROUP BY minute_bucket
		ORDER BY minute_bucket ASC
	`, bucket, avgLatencyExpr, p95LatencyExpr), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
		`avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000`,
		`avgIf(avg, metric_name = 'hikaricp.connections.usage' AND isFinite(avg)) * 1000`,
	)
	p95LatencyExpr := syncAggregateExpr(
		`maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000`,
		`maxIf(p95, metric_name = 'hikaricp.connections.usage' AND isFinite(p95)) * 1000`,
	)
	queryCountExpr := syncAggregateExpr(
		`if(maxIf(count, metric_name = 'mongodb.driver.commands') > 0, toFloat64(maxIf(count, metric_name = 'mongodb.driver.commands')), NULL)`,
		`if(maxIf(count, metric_name = 'hikaricp.connections.usage') > 0, toFloat64(maxIf(count, metric_name = 'hikaricp.connections.usage')), NULL)`,
	)
	summaryRaw, err := dbutil.QueryMap(r.db, `
		SELECT `+avgLatencyExpr+` as avg_query_latency_ms,
		       `+p95LatencyExpr+` as p95_query_latency_ms,
		       toInt64(coalesce(`+queryCountExpr+`, 0)) as db_span_count,
		       sumIf(value, metric_name = 'cache.hits' AND isFinite(value)) as cache_hits,
		       sumIf(value, metric_name = 'cache.misses' AND isFinite(value)) as cache_misses,
		       avgIf(value, metric_name = 'db.replication.lag.ms' AND isFinite(value)) as avg_replication_lag_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND `+dbAllMetricFilter+`
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	systemRaw, err := dbutil.QueryMaps(r.db, `
		SELECT `+dbSystemExpr+` as db_system,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as mongo_query_count,
		       maxIf(count, metric_name = 'hikaricp.connections.usage') as usage_query_count,
		       avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as mongo_avg_query_latency_ms,
		       avgIf(avg, metric_name = 'hikaricp.connections.usage' AND isFinite(avg)) * 1000 as usage_avg_query_latency_ms,
		       maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000 as mongo_p95_query_latency_ms,
		       maxIf(p95, metric_name = 'hikaricp.connections.usage' AND isFinite(p95)) * 1000 as usage_p95_query_latency_ms,
		       sumIf(value, metric_name = 'db.client.errors') as error_count,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as mongo_span_count,
		       maxIf(count, metric_name = 'hikaricp.connections.usage') as usage_span_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND `+dbAllMetricFilter+`
		GROUP BY db_system
		ORDER BY greatest(ifNull(mongo_query_count, 0), ifNull(usage_query_count, 0)) DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	tableMetricsRaw, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as table_name,
		       service_name,
		       %s as db_system,
		       avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as mongo_avg_query_latency_ms,
		       avgIf(avg, metric_name = 'hikaricp.connections.usage' AND isFinite(avg)) * 1000 as usage_avg_query_latency_ms,
		       maxIf(max, metric_name = 'mongodb.driver.commands' AND isFinite(max)) * 1000 as mongo_max_query_latency_ms,
		       maxIf(max, metric_name = 'hikaricp.connections.usage' AND isFinite(max)) * 1000 as usage_max_query_latency_ms,
		       sumIf(value, metric_name = 'cache.hits' AND isFinite(value)) as cache_hits,
		       sumIf(value, metric_name = 'cache.misses' AND isFinite(value)) as cache_misses,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as mongo_query_count,
		       maxIf(count, metric_name = 'hikaricp.connections.usage') as usage_query_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND `+dbAllMetricFilter+`
		GROUP BY table_name, service_name, db_system
		ORDER BY greatest(ifNull(mongo_avg_query_latency_ms, 0), ifNull(usage_avg_query_latency_ms, 0)) DESC
		LIMIT 50
	`, dbCollectionExpr, dbSystemExpr), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	bucket := satFmtBucketExpr(startMs, endMs)
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
		           nullIf(if(metric_name IN ('kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max'), 'kafka-consumer-lag', ''), ''),
		           'unknown'
		       ) as queue_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.system'), ''),
		           if(metric_name LIKE 'kafka.%%' OR metric_name LIKE 'messaging.kafka.%%', 'kafka', ''),
		           'kafka'
		       ) as messaging_system,
		       avgIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as avg_consumer_lag
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'messaging.kafka.consumer.lag',
		      'messaging.kafka.consumer.records.lag',
		      'messaging.kafka.consumer.records-lag',
		      'messaging.kafka.consumer.records.lag.max',
		      'kafka.consumer.lag',
		      'kafka.consumer.records.lag',
		      'kafka.consumer.records-lag',
		      'kafka.consumer.records.lag.max',
		      'kafka.consumer.fetch.manager.records.lag',
		      'kafka.consumer.fetch.manager.records.lag.max',
		      'kafka.consumer.fetch.records.lag',
		      'kafka.consumer.fetch.records.lag.max',
		      'executor.queued'
		  )
		GROUP BY time_bucket, service_name, queue_name, messaging_system
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC
	`, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	bucket := satFmtBucketExpr(startMs, endMs)
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
		       avgIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as avg_queue_depth
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'queue.depth',
		      'messaging.queue.depth',
		      'executor.queued'
		  )
		GROUP BY time_bucket, service_name, queue_name, messaging_system
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC
	`, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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

	topQueuesRaw, err := dbutil.QueryMaps(r.db, `
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
		           nullIf(if(metric_name IN ('kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max'), 'kafka-consumer-lag', ''), ''),
		           'unknown'
		       ) as queue_name,
		       if(service_name != '', service_name, 'unknown') as service_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.system'), ''),
		           if(metric_name LIKE 'kafka.%' OR metric_name LIKE 'messaging.kafka.%', 'kafka', ''),
		           'kafka'
		       ) as messaging_system,
		       avgIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as avg_queue_depth,
		       maxIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as max_consumer_lag,
		       maxIf(toFloat64(count), metric_name = 'spring.kafka.template') / ? as avg_publish_rate,
		       maxIf(toFloat64(count), metric_name = 'spring.kafka.listener') / ? as avg_receive_rate,
		       toInt64(count()) as sample_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'queue.depth',
		      'messaging.queue.depth',
		      'executor.queued',
		      'spring.kafka.template',
		      'spring.kafka.listener',
		      'messaging.kafka.consumer.lag',
		      'messaging.kafka.consumer.records.lag',
		      'messaging.kafka.consumer.records-lag',
		      'messaging.kafka.consumer.records.lag.max',
		      'kafka.consumer.lag',
		      'kafka.consumer.records.lag',
		      'kafka.consumer.records-lag',
		      'kafka.consumer.records.lag.max',
		      'kafka.consumer.fetch.manager.records.lag',
		      'kafka.consumer.fetch.manager.records.lag.max',
		      'kafka.consumer.fetch.records.lag',
		      'kafka.consumer.fetch.records.lag.max',
		      'messaging.kafka.published',
		      'messaging.kafka.consumed',
		      'app.activity.kafka.published',
		      'app.activity.kafka.consumed'
		  )
		GROUP BY queue_name, service_name, messaging_system
		ORDER BY avg_queue_depth DESC, max_consumer_lag DESC, sample_count DESC
		LIMIT 50
	`, durationSecs, durationSecs, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
