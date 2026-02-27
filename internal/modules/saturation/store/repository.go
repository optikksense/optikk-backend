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

// GetInsightDatabaseCache queries DB query latency and cache-hit insights.
func (r *ClickHouseRepository) GetInsightDatabaseCache(teamUUID string, startMs, endMs int64) (model.DbCacheSummary, []model.DbTableMetric, []model.DbSystemBreakdown, error) {
	summaryRaw, err := dbutil.QueryMap(r.db, `
		SELECT avg(coalesce(nullIf(JSONExtractFloat(attributes, 'db.query.latency.ms'), 0), toFloat64(duration_ms))) as avg_query_latency_ms,
		       quantile(0.95)(coalesce(nullIf(JSONExtractFloat(attributes, 'db.query.latency.ms'), 0), toFloat64(duration_ms))) as p95_query_latency_ms,
		       COUNT(*) as db_span_count,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'true', 1, 0))  as cache_hits,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'false', 1, 0)) as cache_misses,
		       avg(JSONExtractFloat(attributes, 'db.replication.lag.ms')) as avg_replication_lag_ms
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		  AND (
		      JSONExtractString(attributes, 'db.system') != ''
		      OR JSONExtractString(attributes, 'db.name') != ''
		      OR lower(operation_name) LIKE '%mongo%'
		      OR lower(operation_name) LIKE '%db%'
		      OR lower(operation_name) LIKE '%sql%'
		      OR lower(service_name) LIKE '%mongo%'
		      OR lower(service_name) LIKE '%db%'
		      OR lower(service_name) LIKE '%sql%'
		  )
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return model.DbCacheSummary{}, nil, nil, err
	}

	summary := model.DbCacheSummary{
		AvgQueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["avg_query_latency_ms"]),
		P95QueryLatencyMs:   dbutil.NullableFloat64FromAny(summaryRaw["p95_query_latency_ms"]),
		DbSpanCount:         dbutil.Int64FromAny(summaryRaw["db_span_count"]),
		CacheHits:           dbutil.Int64FromAny(summaryRaw["cache_hits"]),
		CacheMisses:         dbutil.Int64FromAny(summaryRaw["cache_misses"]),
		AvgReplicationLagMs: dbutil.NullableFloat64FromAny(summaryRaw["avg_replication_lag_ms"]),
	}

	tableMetricsRaw, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'db.sql.table'), ''),
		           nullIf(JSONExtractString(attributes, 'db.mongodb.collection'), ''),
		           nullIf(JSONExtractString(attributes, 'db.collection.name'), ''),
		           nullIf(JSONExtractString(attributes, 'http.route'), ''),
		           nullIf(operation_name, ''),
		           'unknown'
		       ) as table_name,
		       service_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'db.system'), ''),
		           if(
		               lower(operation_name) LIKE '%mongo%' OR lower(service_name) LIKE '%mongo%',
		               'mongodb',
		               if(
		                   lower(operation_name) LIKE '%redis%' OR lower(service_name) LIKE '%redis%',
		                   'redis',
		                   if(
		                       lower(operation_name) LIKE '%sql%' OR lower(operation_name) LIKE '%jdbc%' OR lower(service_name) LIKE '%mysql%' OR lower(service_name) LIKE '%postgres%',
		                       'sql',
		                       'unknown'
		                   )
		               )
		           )
		       ) as db_system,
		       avg(coalesce(nullIf(JSONExtractFloat(attributes, 'db.query.latency.ms'), 0), toFloat64(duration_ms))) as avg_query_latency_ms,
		       max(coalesce(nullIf(JSONExtractFloat(attributes, 'db.query.latency.ms'), 0), toFloat64(duration_ms))) as max_query_latency_ms,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'true',  1, 0)) as cache_hits,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'false', 1, 0)) as cache_misses,
		       COUNT(*) as query_count
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		  AND (
		      JSONExtractString(attributes, 'db.system') != ''
		      OR JSONExtractString(attributes, 'db.name') != ''
		      OR lower(operation_name) LIKE '%mongo%'
		      OR lower(operation_name) LIKE '%db%'
		      OR lower(operation_name) LIKE '%sql%'
		      OR lower(service_name) LIKE '%mongo%'
		      OR lower(service_name) LIKE '%db%'
		      OR lower(service_name) LIKE '%sql%'
		  )
		GROUP BY table_name, service_name, db_system
		ORDER BY avg_query_latency_ms DESC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.DbCacheSummary{}, nil, nil, err
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

	// System breakdown query — groups by db.system to show per-database-type stats
	systemRaw, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'db.system'), ''),
		           if(
		               lower(operation_name) LIKE '%mongo%' OR lower(service_name) LIKE '%mongo%',
		               'mongodb',
		               if(
		                   lower(operation_name) LIKE '%redis%' OR lower(service_name) LIKE '%redis%',
		                   'redis',
		                   if(
		                       lower(operation_name) LIKE '%sql%' OR lower(operation_name) LIKE '%jdbc%' OR lower(service_name) LIKE '%mysql%' OR lower(service_name) LIKE '%postgres%',
		                       'sql',
		                       'unknown'
		                   )
		               )
		           )
		       ) as db_system,
		       COUNT(*) as query_count,
		       avg(coalesce(nullIf(JSONExtractFloat(attributes, 'db.query.latency.ms'), 0), toFloat64(duration_ms))) as avg_query_latency_ms,
		       quantile(0.95)(coalesce(nullIf(JSONExtractFloat(attributes, 'db.query.latency.ms'), 0), toFloat64(duration_ms))) as p95_query_latency_ms,
		       sum(if(status = 'ERROR', 1, 0)) as error_count,
		       COUNT(*) as span_count
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		  AND (
		      JSONExtractString(attributes, 'db.system') != ''
		      OR JSONExtractString(attributes, 'db.name') != ''
		      OR lower(operation_name) LIKE '%mongo%'
		      OR lower(operation_name) LIKE '%db%'
		      OR lower(operation_name) LIKE '%sql%'
		      OR lower(service_name) LIKE '%mongo%'
		      OR lower(service_name) LIKE '%db%'
		      OR lower(service_name) LIKE '%sql%'
		  )
		GROUP BY db_system
		ORDER BY query_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.DbCacheSummary{}, nil, nil, err
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

	return summary, tableMetrics, systemBreakdown, err
}

// GetInsightMessagingQueue queries queue depth, lag, and rates.
func (r *ClickHouseRepository) GetInsightMessagingQueue(teamUUID string, startMs, endMs int64) (model.MqSummary, []model.MqBucket, []model.MqTopQueue, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}

	summaryRaw, err := dbutil.QueryMap(r.db, `
		SELECT round(if(sum(queue_depth_samples) > 0, sum(queue_depth_sum) / sum(queue_depth_samples), NULL), 2) as avg_queue_depth,
		       round(max(max_queue_depth), 2) as max_queue_depth,
		       round(if(sum(consumer_lag_samples) > 0, sum(consumer_lag_sum) / sum(consumer_lag_samples), NULL), 2) as avg_consumer_lag,
		       round(max(max_consumer_lag), 2) as max_consumer_lag,
		       round(sum(publish_events) / ?, 2) as avg_publish_rate,
		       round(sum(receive_events) / ?, 2) as avg_receive_rate,
		       round(sum(processing_errors), 2) as processing_errors
		FROM (
		    SELECT sumIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_sum,
		           countIf(JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_samples,
		           maxIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as max_queue_depth,
		           sumIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_sum,
		           countIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_samples,
		           maxIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as max_consumer_lag,
		           toFloat64(sum(
		               if(
		                   span_kind = 'PRODUCER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		                   OR (
		                       upper(http_method) = 'POST'
		                       AND (
		                           positionCaseInsensitive(operation_name, '/api/activities') > 0
		                           OR positionCaseInsensitive(http_url, '/api/activities') > 0
		                       )
		                   ),
		                   1, 0
		               )
		           )) as publish_events,
		           toFloat64(sum(
		               if(
		                   span_kind = 'CONSUMER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		                   OR (
		                       upper(http_method) = 'GET'
		                       AND (
		                           positionCaseInsensitive(operation_name, '/api/activities') > 0
		                           OR positionCaseInsensitive(http_url, '/api/activities') > 0
		                       )
		                   ),
		                   1, 0
		               )
		           )) as receive_events,
		           sum(if(isFinite(JSONExtractFloat(attributes, 'messaging.error.count')), JSONExtractFloat(attributes, 'messaging.error.count'), 0.0)) as processing_errors
		    FROM spans
		    WHERE team_id = ? AND start_time BETWEEN ? AND ?

		    UNION ALL

		    SELECT sumIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_sum,
		           countIf(metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_samples,
		           maxIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as max_queue_depth,
		           sumIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as consumer_lag_sum,
		           countIf(metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as consumer_lag_samples,
		           maxIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as max_consumer_lag,
		           toFloat64(
		               sumIf(
		                   if(count > 0, count, 1),
		                   metric_name = 'http.server.request.count'
		                   AND upper(http_method) = 'POST'
		                   AND positionCaseInsensitive(JSONExtractString(attributes, 'http.route'), '/api/activities') > 0
		               )
		               + maxIf(value, metric_name IN ('messaging.kafka.published', 'app.activity.kafka.published') AND isFinite(value))
		           ) as publish_events,
		           toFloat64(
		               sumIf(
		                   if(count > 0, count, 1),
		                   metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max')
		               )
		               + maxIf(value, metric_name IN ('messaging.kafka.consumed', 'app.activity.kafka.consumed') AND isFinite(value))
		           ) as receive_events,
		           sumIf(value, metric_name = 'logback.events' AND lower(JSONExtractString(attributes, 'level')) IN ('error', 'fatal') AND isFinite(value)) as processing_errors
		    FROM metrics
		    WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		) merged
	`, durationSecs, durationSecs,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.MqSummary{}, nil, nil, err
	}

	summary := model.MqSummary{
		AvgQueueDepth:    dbutil.NullableFloat64FromAny(summaryRaw["avg_queue_depth"]),
		MaxQueueDepth:    dbutil.NullableFloat64FromAny(summaryRaw["max_queue_depth"]),
		AvgConsumerLag:   dbutil.NullableFloat64FromAny(summaryRaw["avg_consumer_lag"]),
		MaxConsumerLag:   dbutil.NullableFloat64FromAny(summaryRaw["max_consumer_lag"]),
		AvgPublishRate:   dbutil.Float64FromAny(summaryRaw["avg_publish_rate"]),
		AvgReceiveRate:   dbutil.Float64FromAny(summaryRaw["avg_receive_rate"]),
		ProcessingErrors: dbutil.Float64FromAny(summaryRaw["processing_errors"]),
	}

	timeseriesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT time_bucket,
		       service_name,
		       queue_name,
		       any(messaging_system) as messaging_system,
		       round(if(sum(queue_depth_samples) > 0, sum(queue_depth_sum) / sum(queue_depth_samples), NULL), 2) as avg_queue_depth,
		       round(if(sum(consumer_lag_samples) > 0, sum(consumer_lag_sum) / sum(consumer_lag_samples), NULL), 2) as avg_consumer_lag,
		       round(sum(publish_events) / 60.0, 2) as avg_publish_rate,
		       round(sum(receive_events) / 60.0, 2) as avg_receive_rate
		FROM (
		    SELECT formatDateTime(toStartOfMinute(start_time), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
		           if(service_name != '', service_name, 'unknown') as service_name,
		           coalesce(
		               nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.kafka.destination'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.kafka.topic'), ''),
		               nullIf(JSONExtractString(attributes, 'kafka.topic'), ''),
		               nullIf(JSONExtractString(attributes, 'topic'), ''),
		               nullIf(if(positionCaseInsensitive(operation_name, '/api/activities') > 0 OR positionCaseInsensitive(http_url, '/api/activities') > 0, 'activities-topic', ''), ''),
		               nullIf(if(span_kind IN ('PRODUCER', 'CONSUMER'), 'activities-topic', ''), ''),
		               'unknown'
		           ) as queue_name,
		           coalesce(nullIf(JSONExtractString(attributes, 'messaging.system'), ''), 'kafka') as messaging_system,
		           sumIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_sum,
		           countIf(JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_samples,
		           sumIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_sum,
		           countIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as consumer_lag_samples,
		           toFloat64(sum(
		               if(
		                   span_kind = 'PRODUCER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		                   OR (
		                       upper(http_method) = 'POST'
		                       AND (
		                           positionCaseInsensitive(operation_name, '/api/activities') > 0
		                           OR positionCaseInsensitive(http_url, '/api/activities') > 0
		                       )
		                   ),
		                   1, 0
		               )
		           )) as publish_events,
		           toFloat64(sum(
		               if(
		                   span_kind = 'CONSUMER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		                   OR (
		                       upper(http_method) = 'GET'
		                       AND (
		                           positionCaseInsensitive(operation_name, '/api/activities') > 0
		                           OR positionCaseInsensitive(http_url, '/api/activities') > 0
		                       )
		                   ),
		                   1, 0
		               )
		           )) as receive_events
		    FROM spans
		    WHERE team_id = ? AND start_time BETWEEN ? AND ?
		    GROUP BY 1, 2, 3, 4

		    UNION ALL

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
		               nullIf(if((metric_name = 'http.server.request.count' AND positionCaseInsensitive(JSONExtractString(attributes, 'http.route'), '/api/activities') > 0) OR metric_name IN ('messaging.kafka.published', 'messaging.kafka.consumed', 'app.activity.kafka.published', 'app.activity.kafka.consumed'), 'activities-topic', ''), ''),
		               'unknown'
		           ) as queue_name,
		           coalesce(
		               nullIf(JSONExtractString(attributes, 'messaging.system'), ''),
		               if(metric_name LIKE 'kafka.%' OR metric_name LIKE 'messaging.kafka.%', 'kafka', ''),
		               'kafka'
		           ) as messaging_system,
		           sumIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_sum,
		           countIf(metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_samples,
		           sumIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as consumer_lag_sum,
		           countIf(metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as consumer_lag_samples,
		           toFloat64(
		               sumIf(
		                   if(count > 0, count, 1),
		                   metric_name = 'http.server.request.count'
		                   AND upper(http_method) = 'POST'
		                   AND positionCaseInsensitive(JSONExtractString(attributes, 'http.route'), '/api/activities') > 0
		               )
		               + maxIf(value, metric_name IN ('messaging.kafka.published', 'app.activity.kafka.published') AND isFinite(value))
		           ) as publish_events,
		           toFloat64(
		               sumIf(
		                   if(count > 0, count, 1),
		                   metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max')
		               )
		               + maxIf(value, metric_name IN ('messaging.kafka.consumed', 'app.activity.kafka.consumed') AND isFinite(value))
		           ) as receive_events
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
		    GROUP BY 1, 2, 3, 4
		) merged
		GROUP BY time_bucket, service_name, queue_name
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.MqSummary{}, nil, nil, err
	}

	timeseries := make([]model.MqBucket, len(timeseriesRaw))
	for i, row := range timeseriesRaw {
		timeseries[i] = model.MqBucket{
			Timestamp:       dbutil.StringFromAny(row["time_bucket"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgQueueDepth:   dbutil.NullableFloat64FromAny(row["avg_queue_depth"]),
			AvgConsumerLag:  dbutil.NullableFloat64FromAny(row["avg_consumer_lag"]),
			AvgPublishRate:  dbutil.Float64FromAny(row["avg_publish_rate"]),
			AvgReceiveRate:  dbutil.Float64FromAny(row["avg_receive_rate"]),
		}
	}

	topQueuesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT queue_name,
		       service_name,
		       any(messaging_system) as messaging_system,
		       round(if(sum(queue_depth_samples) > 0, sum(queue_depth_sum) / sum(queue_depth_samples), NULL), 2) as avg_queue_depth,
		       round(max(max_consumer_lag), 2) as max_consumer_lag,
		       round(sum(publish_events) / ?, 2) as avg_publish_rate,
		       round(sum(receive_events) / ?, 2) as avg_receive_rate,
		       toInt64(sum(sample_count)) as sample_count
		FROM (
		    SELECT coalesce(
		               nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination.name'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.kafka.destination'), ''),
		               nullIf(JSONExtractString(attributes, 'messaging.kafka.topic'), ''),
		               nullIf(JSONExtractString(attributes, 'kafka.topic'), ''),
		               nullIf(JSONExtractString(attributes, 'topic'), ''),
		               nullIf(if(positionCaseInsensitive(operation_name, '/api/activities') > 0 OR positionCaseInsensitive(http_url, '/api/activities') > 0, 'activities-topic', ''), ''),
		               nullIf(if(span_kind IN ('PRODUCER', 'CONSUMER'), 'activities-topic', ''), ''),
		               'unknown'
		           ) as queue_name,
		           if(service_name != '', service_name, 'unknown') as service_name,
		           coalesce(nullIf(JSONExtractString(attributes, 'messaging.system'), ''), 'kafka') as messaging_system,
		           sumIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_sum,
		           countIf(JSONExtractFloat(attributes, 'queue.depth') > 0) as queue_depth_samples,
		           maxIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as max_consumer_lag,
		           toFloat64(sum(
		               if(
		                   span_kind = 'PRODUCER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		                   OR (
		                       upper(http_method) = 'POST'
		                       AND (
		                           positionCaseInsensitive(operation_name, '/api/activities') > 0
		                           OR positionCaseInsensitive(http_url, '/api/activities') > 0
		                       )
		                   ),
		                   1, 0
		               )
		           )) as publish_events,
		           toFloat64(sum(
		               if(
		                   span_kind = 'CONSUMER'
		                   OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		                   OR (
		                       upper(http_method) = 'GET'
		                       AND (
		                           positionCaseInsensitive(operation_name, '/api/activities') > 0
		                           OR positionCaseInsensitive(http_url, '/api/activities') > 0
		                       )
		                   ),
		                   1, 0
		               )
		           )) as receive_events,
		           toInt64(count()) as sample_count
		    FROM spans
		    WHERE team_id = ? AND start_time BETWEEN ? AND ?
		    GROUP BY queue_name, service_name, messaging_system

		    UNION ALL

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
		           sumIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_sum,
		           countIf(metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as queue_depth_samples,
		           maxIf(value, metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max', 'executor.queued') AND isFinite(value)) as max_consumer_lag,
		           toFloat64(
		               sumIf(
		                   if(count > 0, count, 1),
		                   metric_name = 'http.server.request.count'
		                   AND upper(http_method) = 'POST'
		                   AND positionCaseInsensitive(JSONExtractString(attributes, 'http.route'), '/api/activities') > 0
		               )
		               + maxIf(value, metric_name IN ('messaging.kafka.published', 'app.activity.kafka.published') AND isFinite(value))
		           ) as publish_events,
		           toFloat64(
		               sumIf(
		                   if(count > 0, count, 1),
		                   metric_name IN ('messaging.kafka.consumer.lag', 'messaging.kafka.consumer.records.lag', 'messaging.kafka.consumer.records-lag', 'messaging.kafka.consumer.records.lag.max', 'kafka.consumer.lag', 'kafka.consumer.records.lag', 'kafka.consumer.records-lag', 'kafka.consumer.records.lag.max', 'kafka.consumer.fetch.manager.records.lag', 'kafka.consumer.fetch.manager.records.lag.max', 'kafka.consumer.fetch.records.lag', 'kafka.consumer.fetch.records.lag.max')
		               )
		               + maxIf(value, metric_name IN ('messaging.kafka.consumed', 'app.activity.kafka.consumed') AND isFinite(value))
		           ) as receive_events,
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
		) merged
		GROUP BY queue_name, service_name
		ORDER BY avg_queue_depth DESC, max_consumer_lag DESC, sample_count DESC
		LIMIT 50
	`, durationSecs, durationSecs,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.MqSummary{}, nil, nil, err
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

	return summary, timeseries, topQueues, nil
}
