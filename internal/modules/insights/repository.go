package insights

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for insights.
type Repository struct {
	db dbutil.Querier
}

// NewRepository creates a new Insights Repository.
func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

// GetInsightResourceUtilization queries CPU/memory/disk/network utilization by service and instance.
func (r *Repository) GetInsightResourceUtilization(teamUUID string, startMs, endMs int64) ([]map[string]any, []map[string]any, []map[string]any, []map[string]any, error) {
	byService, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       if(
		           countIf(metric_name = 'system.cpu.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.cpu.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_cpu_util,
		       if(
		           countIf(metric_name = 'system.memory.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_memory_util,
		       if(
		           countIf(metric_name = 'system.disk.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.disk.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.disk.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.disk.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_disk_util,
		       if(
		           countIf(metric_name = 'system.network.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.network.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.network.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.network.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.network.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_network_util,
		       if(
		           countIf(metric_name = 'db.connection.pool.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'db.connection.pool.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_connection_pool_util,
		       COUNT(*) as sample_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN (
		          'system.cpu.utilization',
		          'system.memory.utilization',
		          'system.disk.utilization',
		          'system.network.utilization',
		          'db.connection.pool.utilization'
		      )
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.network.utilization') > 0
		      OR JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		  )
		GROUP BY service_name
		HAVING service_name != ''
		ORDER BY service_name ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	byInstance, err := dbutil.QueryMaps(r.db, `
		SELECT host, pod, container, service_name,
		       if(
		           countIf(metric_name = 'system.cpu.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.cpu.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_cpu_util,
		       if(
		           countIf(metric_name = 'system.memory.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_memory_util,
		       if(
		           countIf(metric_name = 'system.disk.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.disk.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.disk.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.disk.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.disk.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_disk_util,
		       if(
		           countIf(metric_name = 'system.network.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.network.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.network.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.network.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.network.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.network.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_network_util,
		       if(
		           countIf(metric_name = 'db.connection.pool.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'db.connection.pool.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'db.connection_pool.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_connection_pool_util,
		       COUNT(*) as sample_count
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN (
		          'system.cpu.utilization',
		          'system.memory.utilization',
		          'system.disk.utilization',
		          'system.network.utilization',
		          'db.connection.pool.utilization'
		      )
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.disk.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.network.utilization') > 0
		      OR JSONExtractFloat(attributes, 'db.connection_pool.utilization') > 0
		  )
		GROUP BY host, pod, container, service_name
		HAVING service_name != ''
		ORDER BY sample_count DESC
		LIMIT 200
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	infra, err := dbutil.QueryMaps(r.db, `
		SELECT host, pod, container,
		       COUNT(*) as span_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       avg(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       arrayStringConcat(groupArray(DISTINCT service_name), ',') as services_csv
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ? AND host != ''
		GROUP BY host, pod, container
		ORDER BY span_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Timeseries grouped by service_name (not pod, since pods may be empty).
	// Return RFC3339-like UTC timestamps so frontend bucket alignment matches
	// other timeseries endpoints (e.g. services/timeseries).
	timeseries, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
		       service_name as pod,
		       if(
		           countIf(metric_name = 'system.cpu.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.cpu.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.cpu.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.cpu.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_cpu_util,
		       if(
		           countIf(metric_name = 'system.memory.utilization') > 0,
		           avgIf(if(value <= 1.0, value * 100.0, value), metric_name = 'system.memory.utilization'),
		           if(
		               countIf(JSONExtractFloat(attributes, 'system.memory.utilization') > 0) > 0,
		               avgIf(
		                   if(
		                       JSONExtractFloat(attributes, 'system.memory.utilization') <= 1.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization') * 100.0,
		                       JSONExtractFloat(attributes, 'system.memory.utilization')
		                   ),
		                   JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		               ),
		               NULL
		           )
		       ) as avg_memory_util
		FROM metrics
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		  AND (
		      metric_name IN ('system.cpu.utilization', 'system.memory.utilization')
		      OR JSONExtractFloat(attributes, 'system.cpu.utilization') > 0
		      OR JSONExtractFloat(attributes, 'system.memory.utilization') > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return byService, byInstance, infra, timeseries, nil
}

// GetInsightSloSli queries SLO compliance status and timeseries.
func (r *Repository) GetInsightSloSli(teamUUID string, startMs, endMs int64, serviceName string) (map[string]any, []map[string]any, error) {
	query1 := `
		SELECT sum(count) as total_requests,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count)>0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms,
		       avg(p95) as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?
	`
	args1 := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query1 += ` AND service_name = ?`
		args1 = append(args1, serviceName)
	}

	summary, err := dbutil.QueryMap(r.db, query1, args1...)
	if err != nil {
		return nil, nil, err
	}

	query2 := `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00') as time_bucket,
		       sum(count) as request_count,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count)>0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?
	`
	args2 := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query2 += ` AND service_name = ?`
		args2 = append(args2, serviceName)
	}
	query2 += ` GROUP BY 1 ORDER BY 1 ASC`

	timeseries, err := dbutil.QueryMaps(r.db, query2, args2...)

	return summary, timeseries, err
}

// GetInsightLogsStream queries log stream, volume trend, and facets.
func (r *Repository) GetInsightLogsStream(teamUUID string, startMs, endMs int64, limit int) ([]map[string]any, int64, []map[string]any, []map[string]any, []map[string]any, error) {
	stream, err := dbutil.QueryMaps(r.db, `
		SELECT timestamp, level, service_name, logger, message, trace_id, span_id,
		       host, pod, container, thread, exception
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC
		LIMIT ?
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit)
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE team_id = ? AND timestamp BETWEEN ? AND ?`,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	volume, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00') as time_bucket,
		       COUNT(*) as log_count,
		       sum(if(trace_id != '', 1, 0)) as correlated_log_count
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY 1
		ORDER BY 1 ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	levelFacets, err := dbutil.QueryMaps(r.db, `
		SELECT level, COUNT(*) as count FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY level ORDER BY count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	serviceFacets, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, COUNT(*) as count FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		GROUP BY service_name ORDER BY count DESC LIMIT 20
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	return stream, total, volume, levelFacets, serviceFacets, err
}

// GetInsightDatabaseCache queries DB query latency and cache-hit insights.
func (r *Repository) GetInsightDatabaseCache(teamUUID string, startMs, endMs int64) (map[string]any, []map[string]any, []map[string]any, error) {
	summary, err := dbutil.QueryMap(r.db, `
		SELECT avg(JSONExtractFloat(attributes, 'db.query.latency.ms'))  as avg_query_latency_ms,
		       quantile(0.95)(JSONExtractFloat(attributes, 'db.query.latency.ms')) as p95_query_latency_ms,
		       sum(if(lower(operation_name) LIKE '%sql%' OR lower(operation_name) LIKE '%db%', 1, 0)) as db_span_count,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'true', 1, 0))  as cache_hits,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'false', 1, 0)) as cache_misses,
		       avg(JSONExtractFloat(attributes, 'db.replication.lag.ms')) as avg_replication_lag_ms
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, err
	}

	tableMetrics, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(nullIf(JSONExtractString(attributes, 'db.sql.table'), ''), 'unknown') as table_name,
		       service_name,
		       avg(JSONExtractFloat(attributes, 'db.query.latency.ms'))  as avg_query_latency_ms,
		       max(JSONExtractFloat(attributes, 'db.query.latency.ms'))  as max_query_latency_ms,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'true',  1, 0)) as cache_hits,
		       sum(if(JSONExtractString(attributes, 'cache.hit') = 'false', 1, 0)) as cache_misses,
		       COUNT(*) as query_count
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		  AND JSONExtractString(attributes, 'db.system') != ''
		GROUP BY table_name, service_name
		ORDER BY avg_query_latency_ms DESC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, err
	}

	// Return empty slowLogs since we're removing slow logs from the UI
	slowLogs := []map[string]any{}

	return summary, tableMetrics, slowLogs, err
}

// GetInsightMessagingQueue queries queue depth, lag, and rates.
func (r *Repository) GetInsightMessagingQueue(teamUUID string, startMs, endMs int64) (map[string]any, []map[string]any, []map[string]any, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}

	summary, err := dbutil.QueryMap(r.db, `
		SELECT avg(JSONExtractFloat(attributes, 'queue.depth'))                    as avg_queue_depth,
		       max(JSONExtractFloat(attributes, 'queue.depth'))                    as max_queue_depth,
		       avg(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'))   as avg_consumer_lag,
		       max(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag'))   as max_consumer_lag,
		       sum(
		           if(
		               span_kind = 'PRODUCER'
		               OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		               OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'POST'),
		               1, 0
		           )
		       ) / ? as avg_publish_rate,
		       sum(
		           if(
		               span_kind = 'CONSUMER'
		               OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		               OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'GET'),
		               1, 0
		           )
		       ) / ? as avg_receive_rate,
		       sum(JSONExtractFloat(attributes, 'messaging.error.count')) as processing_errors
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
	`, durationSecs, durationSecs, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, err
	}

	timeseries, err := dbutil.QueryMaps(r.db, `
		SELECT formatDateTime(toStartOfMinute(start_time), '%Y-%m-%dT%H:%i:%SZ') as time_bucket,
		       service_name,
		       coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		           'unknown'
		       ) as queue_name,
		       avg(JSONExtractFloat(attributes, 'queue.depth'))                  as avg_queue_depth,
		       avg(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag')) as avg_consumer_lag,
		       sum(
		           if(
		               span_kind = 'PRODUCER'
		               OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		               OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'POST'),
		               1, 0
		           )
		       ) / 60.0 as avg_publish_rate,
		       sum(
		           if(
		               span_kind = 'CONSUMER'
		               OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		               OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'GET'),
		               1, 0
		           )
		       ) / 60.0 as avg_receive_rate
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		GROUP BY 1, 2, 3
		ORDER BY 1 ASC, 2 ASC, 3 ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, nil, err
	}

	topQueues, err := dbutil.QueryMaps(r.db, `
		SELECT coalesce(
		           nullIf(JSONExtractString(attributes, 'messaging.queue.name'), ''),
		           nullIf(JSONExtractString(attributes, 'messaging.destination'), ''),
		           'unknown'
		       ) as queue_name,
		       service_name,
		       avg(JSONExtractFloat(attributes, 'queue.depth'))                  as avg_queue_depth,
		       max(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag')) as max_consumer_lag,
		       sum(
		           if(
		               span_kind = 'PRODUCER'
		               OR JSONExtractString(attributes, 'messaging.operation') = 'publish'
		               OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'POST'),
		               1, 0
		           )
		       ) / ? as avg_publish_rate,
		       sum(
		           if(
		               span_kind = 'CONSUMER'
		               OR JSONExtractString(attributes, 'messaging.operation') = 'receive'
		               OR (JSONExtractFloat(attributes, 'queue.depth') > 0 AND upper(http_method) = 'GET'),
		               1, 0
		           )
		       ) / ? as avg_receive_rate,
		       COUNT(*) as sample_count
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		GROUP BY queue_name, service_name
		ORDER BY avg_queue_depth DESC, max_consumer_lag DESC
		LIMIT 50
	`, durationSecs, durationSecs, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	return summary, timeseries, topQueues, err
}
