package store

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/saturation/model"
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
	nullIf(JSONExtractString(attributes, 'db.statement'), ''),
	'unknown'
)`

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

func (r *ClickHouseRepository) GetKafkaQueueLag(teamUUID string, startMs, endMs int64) ([]model.KafkaQueueLag, error) {
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

	results := make([]model.KafkaQueueLag, len(rows))
	for i, row := range rows {
		results[i] = model.KafkaQueueLag{
			Queue:          dbutil.StringFromAny(row["queue"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgConsumerLag: dbutil.Float64FromAny(row["avg_consumer_lag"]),
			MaxConsumerLag: dbutil.Float64FromAny(row["max_consumer_lag"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetKafkaProductionRate(teamUUID string, startMs, endMs int64) ([]model.KafkaProductionRate, error) {
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

	results := make([]model.KafkaProductionRate, len(rows))
	for i, row := range rows {
		results[i] = model.KafkaProductionRate{
			Queue:          dbutil.StringFromAny(row["queue"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgPublishRate: dbutil.Float64FromAny(row["avg_publish_rate"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetKafkaConsumptionRate(teamUUID string, startMs, endMs int64) ([]model.KafkaConsumptionRate, error) {
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

	results := make([]model.KafkaConsumptionRate, len(rows))
	for i, row := range rows {
		results[i] = model.KafkaConsumptionRate{
			Queue:          dbutil.StringFromAny(row["queue"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgReceiveRate: dbutil.Float64FromAny(row["avg_receive_rate"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetDatabaseQueryByTable(teamUUID string, startMs, endMs int64) ([]model.DatabaseQueryByTable, error) {
	bucket := satBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as table_name,
		       %s as minute_bucket,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as query_count,
		       avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as avg_latency_ms,
		       maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000 as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'mongodb.driver.commands'
		  )
		GROUP BY table_name, minute_bucket
		ORDER BY minute_bucket ASC, table_name ASC
	`, dbCollectionExpr, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	results := make([]model.DatabaseQueryByTable, len(rows))
	for i, row := range rows {
		results[i] = model.DatabaseQueryByTable{
			Table:        dbutil.StringFromAny(row["table_name"]),
			Timestamp:    dbutil.StringFromAny(row["minute_bucket"]),
			QueryCount:   dbutil.Int64FromAny(row["query_count"]),
			AvgLatencyMs: dbutil.Float64FromAny(row["avg_latency_ms"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetDatabaseAvgLatency(teamUUID string, startMs, endMs int64) ([]model.DatabaseAvgLatency, error) {
	bucket := satBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as minute_bucket,
		       avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as avg_latency_ms,
		       maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000 as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'mongodb.driver.commands'
		  )
		GROUP BY minute_bucket
		ORDER BY minute_bucket ASC
	`, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	results := make([]model.DatabaseAvgLatency, len(rows))
	for i, row := range rows {
		results[i] = model.DatabaseAvgLatency{
			Timestamp:    dbutil.StringFromAny(row["minute_bucket"]),
			AvgLatencyMs: dbutil.Float64FromAny(row["avg_latency_ms"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
		}
	}
	return results, nil
}

// GetDatabaseCacheSummary queries DB query latency and cache-hit insights from metrics.
func (r *ClickHouseRepository) GetDatabaseCacheSummary(teamUUID string, startMs, endMs int64) (model.DbCacheSummary, error) {
	summaryRaw, err := dbutil.QueryMap(r.db, `
		SELECT avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as avg_query_latency_ms,
		       maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000 as p95_query_latency_ms,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as db_span_count,
		       sumIf(value, metric_name = 'cache.hits' AND isFinite(value)) as cache_hits,
		       sumIf(value, metric_name = 'cache.misses' AND isFinite(value)) as cache_misses,
		       avgIf(value, metric_name = 'db.replication.lag.ms' AND isFinite(value)) as avg_replication_lag_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'mongodb.driver.commands',
		      'cache.hits',
		      'cache.misses',
		      'db.replication.lag.ms'
		  )
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return model.DbCacheSummary{}, err
	}

	return model.DbCacheSummary{
		AvgQueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["avg_query_latency_ms"]),
		P95QueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["p95_query_latency_ms"]),
		DbSpanCount:         dbutil.Int64FromAny(summaryRaw["db_span_count"]),
		CacheHits:           dbutil.Int64FromAny(summaryRaw["cache_hits"]),
		CacheMisses:         dbutil.Int64FromAny(summaryRaw["cache_misses"]),
		AvgReplicationLagMs: dbutil.NullableFloat64FromAny(summaryRaw["avg_replication_lag_ms"]),
	}, nil
}

// GetDatabaseSystems queries DB metrics grouped by the database system.
func (r *ClickHouseRepository) GetDatabaseSystems(teamUUID string, startMs, endMs int64) ([]model.DbSystemBreakdown, error) {
	systemRaw, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'db.system'), ''),
		           if(
		               lower(metric_name) LIKE '%mongo%' OR lower(service_name) LIKE '%mongo%',
		               'mongodb',
		               if(
		                   lower(metric_name) LIKE '%redis%' OR lower(service_name) LIKE '%redis%',
		                   'redis',
		                   if(
		                       lower(metric_name) LIKE '%sql%' OR lower(service_name) LIKE '%mysql%' OR lower(service_name) LIKE '%postgres%',
		                       'sql',
		                       'unknown'
		                   )
		               )
		           )
		       ) as db_system,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as query_count,
		       avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as avg_query_latency_ms,
		       maxIf(p95, metric_name = 'mongodb.driver.commands' AND isFinite(p95)) * 1000 as p95_query_latency_ms,
		       sumIf(value, metric_name = 'db.client.errors') as error_count,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as span_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'mongodb.driver.commands',
		      'db.client.errors'
		  )
		GROUP BY db_system
		ORDER BY query_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	systemBreakdown := make([]model.DbSystemBreakdown, len(systemRaw))
	for i, row := range systemRaw {
		systemBreakdown[i] = model.DbSystemBreakdown{
			DbSystem:        dbutil.StringFromAny(row["db_system"]),
			QueryCount:      dbutil.Int64FromAny(row["query_count"]),
			AvgQueryLatency: dbutil.NullableFloat64FromAny(row["avg_query_latency_ms"]),
			P95QueryLatency: dbutil.NullableFloat64FromAny(row["p95_query_latency_ms"]),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			SpanCount:       dbutil.Int64FromAny(row["span_count"]),
		}
	}

	return systemBreakdown, nil
}

// GetDatabaseTopTables queries table-specific DB latency, hits, and misses.
func (r *ClickHouseRepository) GetDatabaseTopTables(teamUUID string, startMs, endMs int64) ([]model.DbTableMetric, error) {
	tableMetricsRaw, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s as table_name,
		       service_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'db.system'), ''),
		           if(
		               lower(metric_name) LIKE '%%mongo%%' OR lower(service_name) LIKE '%%mongo%%',
		               'mongodb',
		               if(
		                   lower(metric_name) LIKE '%%redis%%' OR lower(service_name) LIKE '%%redis%%',
		                   'redis',
		                   if(
		                       lower(metric_name) LIKE '%%sql%%' OR lower(service_name) LIKE '%%mysql%%' OR lower(service_name) LIKE '%%postgres%%',
		                       'sql',
		                       'unknown'
		                   )
		               )
		           )
		       ) as db_system,
		       avgIf(avg, metric_name = 'mongodb.driver.commands' AND isFinite(avg)) * 1000 as avg_query_latency_ms,
		       maxIf(max, metric_name = 'mongodb.driver.commands' AND isFinite(max)) * 1000 as max_query_latency_ms,
		       sumIf(value, metric_name = 'cache.hits' AND isFinite(value)) as cache_hits,
		       sumIf(value, metric_name = 'cache.misses' AND isFinite(value)) as cache_misses,
		       maxIf(count, metric_name = 'mongodb.driver.commands') as query_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'mongodb.driver.commands',
		      'cache.hits',
		      'cache.misses'
		  )
		GROUP BY table_name, service_name, db_system
		ORDER BY avg_query_latency_ms DESC
		LIMIT 50
	`, dbCollectionExpr), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	tableMetrics := make([]model.DbTableMetric, len(tableMetricsRaw))
	for i, row := range tableMetricsRaw {
		tableMetrics[i] = model.DbTableMetric{
			TableName:         dbutil.StringFromAny(row["table_name"]),
			ServiceName:       dbutil.StringFromAny(row["service_name"]),
			DbSystem:          dbutil.StringFromAny(row["db_system"]),
			AvgQueryLatencyMs: dbutil.NullableFloat64FromAny(row["avg_query_latency_ms"]),
			MaxQueryLatencyMs: dbutil.NullableFloat64FromAny(row["max_query_latency_ms"]),
			CacheHits:         dbutil.Int64FromAny(row["cache_hits"]),
			CacheMisses:       dbutil.Int64FromAny(row["cache_misses"]),
			QueryCount:        dbutil.Int64FromAny(row["query_count"]),
		}
	}

	return tableMetrics, nil
}

// GetQueueConsumerLag queries the consumer lag timeseries for queues.
func (r *ClickHouseRepository) GetQueueConsumerLag(teamUUID string, startMs, endMs int64) ([]model.MqBucket, error) {
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

	timeseries := make([]model.MqBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = model.MqBucket{
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
func (r *ClickHouseRepository) GetQueueTopicLag(teamUUID string, startMs, endMs int64) ([]model.MqBucket, error) {
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

	timeseries := make([]model.MqBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = model.MqBucket{
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
func (r *ClickHouseRepository) GetQueueTopQueues(teamUUID string, startMs, endMs int64) ([]model.MqTopQueue, error) {
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

	topQueues := make([]model.MqTopQueue, len(topQueuesRaw))
	for i, row := range topQueuesRaw {
		topQueues[i] = model.MqTopQueue{
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
