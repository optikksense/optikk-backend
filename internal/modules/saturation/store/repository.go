package store

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/saturation/model"
)

// ClickHouseRepository encapsulates saturation data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new saturation repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetKafkaQueueLag(teamUUID string, startMs, endMs int64) ([]model.KafkaQueueLag, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		           'unknown_topic'
		       ) as queue,
		       toStartOfMinute(timestamp) as minute_bucket,
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
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		           'unknown_topic'
		       ) as queue,
		       toStartOfMinute(timestamp) as minute_bucket,
		       avgIf(value, metric_name IN (
		           'messaging.publish.rate',
		           'kafka.producer.record.send.rate',
		           'kafka.producer.record-send-rate'
		       ) AND isFinite(value)) as avg_publish_rate
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'messaging.publish.rate',
		      'kafka.producer.record.send.rate',
		      'kafka.producer.record-send-rate'
		  )
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		           'unknown_topic'
		       ) as queue,
		       toStartOfMinute(timestamp) as minute_bucket,
		       avgIf(value, metric_name IN (
		           'messaging.receive.rate',
		           'kafka.consumer.fetch.rate',
		           'kafka.consumer.records.consumed.rate'
		       ) AND isFinite(value)) as avg_receive_rate
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'messaging.receive.rate',
		      'kafka.consumer.fetch.rate',
		      'kafka.consumer.records.consumed.rate'
		  )
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'db.statement'), ''),
		           nullIf(JSONExtractString(attributes, 'db.sql.table'), ''),
		           nullIf(JSONExtractString(attributes, 'db.collection.name'), ''),
		           'unknown_table'
		       ) as table_name,
		       toStartOfMinute(timestamp) as minute_bucket,
		       sumIf(value, metric_name IN (
		           'db.client.operation.duration',
		           'db.system.queries',
		           'db.client.operations'
		       ) AND isFinite(value)) as query_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'db.client.operation.duration',
		      'db.system.queries',
		      'db.client.operations'
		  )
		GROUP BY table_name, minute_bucket
		ORDER BY minute_bucket ASC, table_name ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	results := make([]model.DatabaseQueryByTable, len(rows))
	for i, row := range rows {
		results[i] = model.DatabaseQueryByTable{
			Table:      dbutil.StringFromAny(row["table_name"]),
			Timestamp:  dbutil.StringFromAny(row["minute_bucket"]),
			QueryCount: dbutil.Int64FromAny(row["query_count"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetDatabaseAvgLatency(teamUUID string, startMs, endMs int64) ([]model.DatabaseAvgLatency, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT toStartOfMinute(timestamp) as minute_bucket,
		       avgIf(value, metric_name IN (
		           'db.client.operation.duration',
		           'db.client.latency'
		       ) AND isFinite(value)) as avg_latency_ms,
		       quantile(0.95)(value) as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'db.client.operation.duration',
		      'db.client.latency'
		  )
		GROUP BY minute_bucket
		ORDER BY minute_bucket ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
		SELECT avgIf(value, metric_name IN ('db.client.operation.duration', 'db.client.latency') AND isFinite(value)) as avg_query_latency_ms,
		       quantileIf(0.95)(value, metric_name IN ('db.client.operation.duration', 'db.client.latency') AND isFinite(value)) as p95_query_latency_ms,
		       countIf(metric_name IN ('db.client.operation.duration', 'db.client.latency')) as db_span_count,
		       sumIf(value, metric_name = 'cache.hits' AND isFinite(value)) as cache_hits,
		       sumIf(value, metric_name = 'cache.misses' AND isFinite(value)) as cache_misses,
		       avgIf(value, metric_name = 'db.replication.lag.ms' AND isFinite(value)) as avg_replication_lag_ms
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'db.client.operation.duration',
		      'db.client.latency',
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
		       countIf(metric_name IN ('db.client.operation.duration', 'db.client.latency', 'db.client.operations', 'db.system.queries')) as query_count,
		       avgIf(value, metric_name IN ('db.client.operation.duration', 'db.client.latency') AND isFinite(value)) as avg_query_latency_ms,
		       quantile(0.95)(value) as p95_query_latency_ms,
		       sumIf(value, metric_name = 'db.client.errors') as error_count,
		       countIf(metric_name IN ('db.client.operation.duration', 'db.client.latency')) as span_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'db.client.operation.duration',
		      'db.client.latency',
		      'db.client.operations',
		      'db.system.queries',
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
	tableMetricsRaw, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'db.statement'), ''),
		           nullIf(JSONExtractString(attributes, 'db.sql.table'), ''),
		           nullIf(JSONExtractString(attributes, 'db.mongodb.collection'), ''),
		           nullIf(JSONExtractString(attributes, 'db.collection.name'), ''),
		           'unknown'
		       ) as table_name,
		       service_name,
		       coalesce(
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
		       avgIf(value, metric_name IN ('db.client.operation.duration', 'db.client.latency') AND isFinite(value)) as avg_query_latency_ms,
		       maxIf(value, metric_name IN ('db.client.operation.duration', 'db.client.latency') AND isFinite(value)) as max_query_latency_ms,
		       sumIf(value, metric_name = 'cache.hits' AND isFinite(value)) as cache_hits,
		       sumIf(value, metric_name = 'cache.misses' AND isFinite(value)) as cache_misses,
		       countIf(metric_name IN ('db.client.operations', 'db.client.operation.duration', 'db.system.queries', 'db.client.latency')) as query_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'db.client.operation.duration',
		      'db.client.latency',
		      'db.client.operations',
		      'db.system.queries',
		      'cache.hits',
		      'cache.misses'
		  )
		GROUP BY table_name, service_name, db_system
		ORDER BY avg_query_latency_ms DESC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	timeseriesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
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
		           nullIf(if(metric_name IN ('kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max'), 'kafka-consumer-lag', ''), ''),
		           'unknown'
		       ) as queue_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.system'), ''),
		           if(metric_name LIKE 'kafka.%' OR metric_name LIKE 'messaging.kafka.%', 'kafka', ''),
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
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
	timeseriesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
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
		           if(metric_name LIKE 'kafka.%' OR metric_name LIKE 'messaging.kafka.%', 'kafka', ''),
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
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
		           nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.destination'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'kafka.topic'), ''),
		           nullIf(JSONExtractString(attributes, 'topic'), ''),
		           nullIf(JSONExtractString(attributes, 'queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'name'), ''),
		           nullIf(if(metric_name IN ('kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max'), 'kafka-consumer-lag', ''), ''),
		           nullIf(if((metric_name = 'http.server.request.count' AND positionCaseInsensitive(JSONExtractString(attributes, 'http.route'), '/api/activities') > 0) OR metric_name IN ('messaging.kafka.published', 'messaging.kafka.consumed', 'app.activity.kafka.published', 'app.activity.kafka.consumed'), 'activities-topic', ''), ''),
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
		       toFloat64(
		           sumIf(
		               if(count > 0, count, 1),
		               metric_name = 'http.server.request.count'
		               AND upper(http_method) = 'POST'
		               AND positionCaseInsensitive(JSONExtractString(attributes, 'http.route'), '/api/activities') > 0
		           )
		           + maxIf(value, metric_name IN ('messaging.kafka.published', 'app.activity.kafka.published') AND isFinite(value))
		       ) / ? as avg_publish_rate,
		       toFloat64(
		           sumIf(
		               if(count > 0, count, 1),
		               metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max')
		           )
		           + maxIf(value, metric_name IN ('messaging.kafka.consumed', 'app.activity.kafka.consumed') AND isFinite(value))
		       ) / ? as avg_receive_rate,
		       toInt64(count()) as sample_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND metric_name IN (
		      'queue.depth',
		      'messaging.queue.depth',
		      'executor.queued',
		      'http.server.request.count',
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
