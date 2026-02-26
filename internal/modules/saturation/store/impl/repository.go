package impl

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

func (r *ClickHouseRepository) GetSaturationMetrics(teamUUID string, startMs, endMs int64) ([]model.SaturationMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		WITH span_metrics AS (
			SELECT if(service_name != '', service_name, 'unknown') as resolved_service_name,
			       COUNT(*) as span_count,
			       AVG(duration_ms) as avg_duration_ms,
			       quantile(0.95)(duration_ms) as p95_duration_ms,
			       MAX(duration_ms) as max_duration_ms,
			       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
			       avgIf(
			           if(
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0,
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0,
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization')
			           ),
			           JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
			       ) as span_avg_db_pool_util,
			       maxIf(
			           if(
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0,
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0,
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization')
			           ),
			           JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
			       ) as span_max_db_pool_util,
			       avgIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as span_avg_consumer_lag,
			       maxIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as span_max_consumer_lag,
			       avgIf(JSONExtractFloat(attributes, 'thread.pool.active'), JSONExtractFloat(attributes, 'thread.pool.active') > 0) as span_avg_thread_pool_active,
			       maxIf(JSONExtractFloat(attributes, 'thread.pool.size'), JSONExtractFloat(attributes, 'thread.pool.size') > 0) as span_max_thread_pool_size,
			       avgIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as span_avg_queue_depth,
			       maxIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as span_max_queue_depth
			FROM spans
			WHERE team_id = ? AND start_time BETWEEN ? AND ?
			GROUP BY resolved_service_name
		),
		metric_metrics AS (
			SELECT coalesce(
			           nullIf(service_name, ''),
			           nullIf(JSONExtractString(attributes, 'service.name'), ''),
			           nullIf(JSONExtractString(attributes, 'attributes.service.name'), ''),
			           'unknown'
			       ) as resolved_service_name,
			       coalesce(
			           if(
			               countIf(metric_name IN ('db.connection.pool.utilization', 'db.connection_pool.utilization') AND isFinite(value)) > 0,
			               avgIf(if(value <= 1.0, value * 100.0, value), metric_name IN ('db.connection.pool.utilization', 'db.connection_pool.utilization') AND isFinite(value)),
			               NULL
			           ),
			           if(
			               sumIf(value, metric_name IN ('hikaricp.connections.max', 'jdbc.connections.max') AND value > 0 AND isFinite(value)) > 0,
			               100.0 * sumIf(value, metric_name IN ('hikaricp.connections.active', 'jdbc.connections.active') AND value >= 0 AND isFinite(value))
			                   / nullIf(sumIf(value, metric_name IN ('hikaricp.connections.max', 'jdbc.connections.max') AND value > 0 AND isFinite(value)), 0),
			               NULL
			           )
			       ) as metric_avg_db_pool_util,
			       coalesce(
			           if(
			               countIf(metric_name IN ('db.connection.pool.utilization', 'db.connection_pool.utilization') AND isFinite(value)) > 0,
			               maxIf(if(value <= 1.0, value * 100.0, value), metric_name IN ('db.connection.pool.utilization', 'db.connection_pool.utilization') AND isFinite(value)),
			               NULL
			           ),
			           if(
			               sumIf(value, metric_name IN ('hikaricp.connections.max', 'jdbc.connections.max') AND value > 0 AND isFinite(value)) > 0,
			               100.0 * sumIf(value, metric_name IN ('hikaricp.connections.active', 'jdbc.connections.active') AND value >= 0 AND isFinite(value))
			                   / nullIf(sumIf(value, metric_name IN ('hikaricp.connections.max', 'jdbc.connections.max') AND value > 0 AND isFinite(value)), 0),
			               NULL
			           )
			       ) as metric_max_db_pool_util,
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
			           'kafka.consumer.fetch.records.lag.max',
			           'executor.queued'
			       ) AND isFinite(value)) as metric_avg_consumer_lag,
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
			           'kafka.consumer.fetch.records.lag.max',
			           'executor.queued'
			       ) AND isFinite(value)) as metric_max_consumer_lag,
			       avgIf(value, metric_name IN ('thread.pool.active', 'executor.active') AND isFinite(value)) as metric_avg_thread_pool_active,
			       maxIf(value, metric_name IN ('thread.pool.size', 'executor.pool.size') AND isFinite(value)) as metric_max_thread_pool_size,
			       avgIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as metric_avg_queue_depth,
			       maxIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as metric_max_queue_depth
			FROM metrics
			WHERE team_id = ? AND timestamp BETWEEN ? AND ?
			  AND metric_name IN (
			      'db.connection.pool.utilization',
			      'db.connection_pool.utilization',
			      'hikaricp.connections.active',
			      'hikaricp.connections.max',
			      'jdbc.connections.active',
			      'jdbc.connections.max',
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
			      'kafka.consumer.fetch.records.lag.max',
			      'thread.pool.active',
			      'thread.pool.size',
			      'executor.active',
			      'executor.pool.size',
			      'queue.depth',
			      'messaging.queue.depth',
			      'executor.queued'
			  )
			GROUP BY resolved_service_name
		)
		SELECT coalesce(
		           nullIf(span_metrics.resolved_service_name, ''),
		           nullIf(metric_metrics.resolved_service_name, ''),
		           'unknown'
		       ) as service_name,
		       coalesce(span_metrics.span_count, 0) as span_count,
		       coalesce(span_metrics.avg_duration_ms, 0) as avg_duration_ms,
		       coalesce(span_metrics.p95_duration_ms, 0) as p95_duration_ms,
		       coalesce(span_metrics.max_duration_ms, 0) as max_duration_ms,
		       coalesce(span_metrics.error_count, 0) as error_count,
		       coalesce(metric_metrics.metric_avg_db_pool_util, span_metrics.span_avg_db_pool_util, 0) as avg_db_pool_util,
		       coalesce(metric_metrics.metric_max_db_pool_util, span_metrics.span_max_db_pool_util, 0) as max_db_pool_util,
		       coalesce(metric_metrics.metric_avg_consumer_lag, span_metrics.span_avg_consumer_lag, 0) as avg_consumer_lag,
		       coalesce(metric_metrics.metric_max_consumer_lag, span_metrics.span_max_consumer_lag, 0) as max_consumer_lag,
		       coalesce(metric_metrics.metric_avg_thread_pool_active, span_metrics.span_avg_thread_pool_active, 0) as avg_thread_pool_active,
		       coalesce(metric_metrics.metric_max_thread_pool_size, span_metrics.span_max_thread_pool_size, 0) as max_thread_pool_size,
		       coalesce(metric_metrics.metric_avg_queue_depth, span_metrics.span_avg_queue_depth, 0) as avg_queue_depth,
		       coalesce(metric_metrics.metric_max_queue_depth, span_metrics.span_max_queue_depth, 0) as max_queue_depth
		FROM span_metrics
		FULL OUTER JOIN metric_metrics ON span_metrics.resolved_service_name = metric_metrics.resolved_service_name
		ORDER BY span_count DESC, service_name ASC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	metrics := make([]model.SaturationMetric, len(rows))
	for i, row := range rows {
		metrics[i] = model.SaturationMetric{
			ServiceName:         dbutil.StringFromAny(row["service_name"]),
			SpanCount:           dbutil.Int64FromAny(row["span_count"]),
			AvgDurationMs:       dbutil.Float64FromAny(row["avg_duration_ms"]),
			P95DurationMs:       dbutil.Float64FromAny(row["p95_duration_ms"]),
			MaxDurationMs:       dbutil.Float64FromAny(row["max_duration_ms"]),
			ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
			AvgDbPoolUtil:       dbutil.Float64FromAny(row["avg_db_pool_util"]),
			MaxDbPoolUtil:       dbutil.Float64FromAny(row["max_db_pool_util"]),
			AvgConsumerLag:      dbutil.Float64FromAny(row["avg_consumer_lag"]),
			MaxConsumerLag:      dbutil.Float64FromAny(row["max_consumer_lag"]),
			AvgThreadPoolActive: dbutil.Float64FromAny(row["avg_thread_pool_active"]),
			MaxThreadPoolSize:   dbutil.Float64FromAny(row["max_thread_pool_size"]),
			AvgQueueDepth:       dbutil.Float64FromAny(row["avg_queue_depth"]),
			MaxQueueDepth:       dbutil.Float64FromAny(row["max_queue_depth"]),
		}
	}
	return metrics, nil
}

func (r *ClickHouseRepository) GetSaturationTimeSeries(teamUUID string, startMs, endMs int64) ([]model.SaturationTimeSeries, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		WITH span_timeseries AS (
			SELECT if(service_name != '', service_name, 'unknown') as resolved_service_name,
			       toStartOfMinute(start_time) as minute_bucket,
			       COUNT(*) as span_count,
			       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
			       AVG(duration_ms) as avg_duration_ms,
			       avgIf(
			           if(
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0,
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0,
			               JSONExtractFloat(attributes, 'db.connection_pool.utilization')
			           ),
			           JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
			       ) as span_avg_db_pool_util,
			       avgIf(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'), JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') > 0) as span_avg_consumer_lag,
			       avgIf(JSONExtractFloat(attributes, 'thread.pool.active'), JSONExtractFloat(attributes, 'thread.pool.active') > 0) as span_avg_thread_active,
			       avgIf(JSONExtractFloat(attributes, 'queue.depth'), JSONExtractFloat(attributes, 'queue.depth') > 0) as span_avg_queue_depth
			FROM spans
			WHERE team_id = ? AND start_time BETWEEN ? AND ?
			GROUP BY resolved_service_name, minute_bucket
		),
		metric_timeseries AS (
			SELECT coalesce(
			           nullIf(service_name, ''),
			           nullIf(JSONExtractString(attributes, 'service.name'), ''),
			           nullIf(JSONExtractString(attributes, 'attributes.service.name'), ''),
			           'unknown'
			       ) as resolved_service_name,
			       toStartOfMinute(timestamp) as minute_bucket,
			       coalesce(
			           if(
			               countIf(metric_name IN ('db.connection.pool.utilization', 'db.connection_pool.utilization') AND isFinite(value)) > 0,
			               avgIf(if(value <= 1.0, value * 100.0, value), metric_name IN ('db.connection.pool.utilization', 'db.connection_pool.utilization') AND isFinite(value)),
			               NULL
			           ),
			           if(
			               sumIf(value, metric_name IN ('hikaricp.connections.max', 'jdbc.connections.max') AND value > 0 AND isFinite(value)) > 0,
			               100.0 * sumIf(value, metric_name IN ('hikaricp.connections.active', 'jdbc.connections.active') AND value >= 0 AND isFinite(value))
			                   / nullIf(sumIf(value, metric_name IN ('hikaricp.connections.max', 'jdbc.connections.max') AND value > 0 AND isFinite(value)), 0),
			               NULL
			           )
			       ) as metric_avg_db_pool_util,
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
			           'kafka.consumer.fetch.records.lag.max',
			           'executor.queued'
			       ) AND isFinite(value)) as metric_avg_consumer_lag,
			       avgIf(value, metric_name IN ('thread.pool.active', 'executor.active') AND isFinite(value)) as metric_avg_thread_active,
			       avgIf(value, metric_name IN ('queue.depth', 'messaging.queue.depth', 'executor.queued') AND isFinite(value)) as metric_avg_queue_depth
			FROM metrics
			WHERE team_id = ? AND timestamp BETWEEN ? AND ?
			  AND metric_name IN (
			      'db.connection.pool.utilization',
			      'db.connection_pool.utilization',
			      'hikaricp.connections.active',
			      'hikaricp.connections.max',
			      'jdbc.connections.active',
			      'jdbc.connections.max',
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
			      'kafka.consumer.fetch.records.lag.max',
			      'thread.pool.active',
			      'executor.active',
			      'queue.depth',
			      'messaging.queue.depth',
			      'executor.queued'
			  )
			GROUP BY resolved_service_name, minute_bucket
		)
		SELECT coalesce(
		           nullIf(span_timeseries.resolved_service_name, ''),
		           nullIf(metric_timeseries.resolved_service_name, ''),
		           'unknown'
		       ) as service_name,
		       formatDateTime(greatest(span_timeseries.minute_bucket, metric_timeseries.minute_bucket), '%Y-%m-%dT%H:%i:%SZ') as timestamp,
		       coalesce(span_timeseries.span_count, 0) as span_count,
		       coalesce(span_timeseries.error_count, 0) as error_count,
		       coalesce(span_timeseries.avg_duration_ms, 0) as avg_duration_ms,
		       coalesce(metric_timeseries.metric_avg_db_pool_util, span_timeseries.span_avg_db_pool_util, 0) as avg_db_pool_util,
		       coalesce(metric_timeseries.metric_avg_consumer_lag, span_timeseries.span_avg_consumer_lag, 0) as avg_consumer_lag,
		       coalesce(metric_timeseries.metric_avg_thread_active, span_timeseries.span_avg_thread_active, 0) as avg_thread_active,
		       coalesce(metric_timeseries.metric_avg_queue_depth, span_timeseries.span_avg_queue_depth, 0) as avg_queue_depth
		FROM span_timeseries
		FULL OUTER JOIN metric_timeseries
			ON span_timeseries.resolved_service_name = metric_timeseries.resolved_service_name
		   AND span_timeseries.minute_bucket = metric_timeseries.minute_bucket
		ORDER BY timestamp ASC, service_name ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	timeseries := make([]model.SaturationTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = model.SaturationTimeSeries{
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			Timestamp:       dbutil.StringFromAny(row["timestamp"]),
			SpanCount:       dbutil.Int64FromAny(row["span_count"]),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			AvgDurationMs:   dbutil.Float64FromAny(row["avg_duration_ms"]),
			AvgDbPoolUtil:   dbutil.Float64FromAny(row["avg_db_pool_util"]),
			AvgConsumerLag:  dbutil.Float64FromAny(row["avg_consumer_lag"]),
			AvgThreadActive: dbutil.Float64FromAny(row["avg_thread_active"]),
			AvgQueueDepth:   dbutil.Float64FromAny(row["avg_queue_depth"]),
		}
	}
	return timeseries, nil
}
