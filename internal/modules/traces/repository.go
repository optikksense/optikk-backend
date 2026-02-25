package traces

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

type TraceFilters struct {
	TeamUUID    string
	StartMs     int64
	EndMs       int64
	Services    []string
	Status      string
	MinDuration string
	MaxDuration string
}

func buildTraceQueryArgs(f TraceFilters) (string, []any) {
	queryFrag := ` WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{f.TeamUUID, dbutil.SqlTime(f.StartMs), dbutil.SqlTime(f.EndMs)}

	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		queryFrag += ` AND service_name IN ` + in
		args = append(args, vals...)
	}
	if f.Status != "" {
		queryFrag += ` AND status = ?`
		args = append(args, f.Status)
	}
	if f.MinDuration != "" {
		queryFrag += ` AND duration_ms >= ?`
		args = append(args, dbutil.MustAtoi64(f.MinDuration, 0))
	}
	if f.MaxDuration != "" {
		queryFrag += ` AND duration_ms <= ?`
		args = append(args, dbutil.MustAtoi64(f.MaxDuration, 0))
	}
	return queryFrag, args
}

func (r *Repository) GetTraces(f TraceFilters, limit, offset int) ([]map[string]any, int64, map[string]any, error) {
	queryFrag, args := buildTraceQueryArgs(f)

	query := `
		SELECT trace_id, service_name, operation_name, start_time, end_time, duration_ms,
		       status, http_method, http_status_code
		FROM spans` + queryFrag + ` ORDER BY start_time DESC LIMIT ? OFFSET ?`
	traceArgs := append(args, limit, offset)

	traces, err := dbutil.QueryMaps(r.db, query, traceArgs...)
	if err != nil {
		return nil, 0, nil, err
	}

	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM spans`+queryFrag, args...)

	summary, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_traces,
		       sum(if(status = 'ERROR', 1, 0)) as error_traces,
		       AVG(duration_ms) as avg_duration,
		       quantile(0.5)(duration_ms) as p50_duration,
		       quantile(0.95)(duration_ms) as p95_duration,
		       quantile(0.99)(duration_ms) as p99_duration
		FROM spans`+queryFrag, args...)
	if err != nil {
		return nil, 0, nil, err
	}

	return traces, total, summary, nil
}

func (r *Repository) GetTraceSpans(teamUUID, traceID string) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT span_id, parent_span_id, operation_name, service_name, span_kind,
		       start_time, end_time, duration_ms, status, status_message,
		       http_method, http_url, http_status_code, host, pod, attributes
		FROM spans
		WHERE team_id = ? AND trace_id = ?
		ORDER BY start_time ASC
	`, teamUUID, traceID)
}

func (r *Repository) GetServiceDependencies(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT p.service_name as source,
		       c.service_name as target,
		       COUNT(*) as call_count
		FROM spans c
		JOIN spans p ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id AND c.team_id = p.team_id
		WHERE c.team_id = ?
		  AND c.start_time BETWEEN ? AND ?
		  AND p.service_name != c.service_name
		GROUP BY p.service_name, c.service_name
		ORDER BY call_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]map[string]any, error) {
	query := `
		SELECT service_name, operation_name, status_message, http_status_code,
		       COUNT(*) as error_count,
		       MAX(start_time) as last_occurrence,
		       MIN(start_time) as first_occurrence,
		       (groupArray(trace_id) as trace_ids)[1] as sample_trace_id
		FROM spans
		WHERE team_id = ? AND status = 'ERROR' AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, operation_name, status_message, http_status_code
	           ORDER BY error_count DESC LIMIT ?`
	args = append(args, limit)

	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetErrorTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]map[string]any, error) {
	query := `
		SELECT service_name,
		       toStartOfMinute(start_time) as timestamp,
		       COUNT(*) as total_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(status='ERROR', 1, 0))*100.0/COUNT(*), 0) as error_rate
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, toStartOfMinute(start_time)
	           ORDER BY timestamp ASC`
	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetLatencyHistogram(teamUUID string, startMs, endMs int64, serviceName, operationName string) ([]map[string]any, error) {
	query := `
		SELECT
			CASE
				WHEN duration_ms < 10 THEN '0-10ms'
				WHEN duration_ms < 25 THEN '10-25ms'
				WHEN duration_ms < 50 THEN '25-50ms'
				WHEN duration_ms < 100 THEN '50-100ms'
				WHEN duration_ms < 250 THEN '100-250ms'
				WHEN duration_ms < 500 THEN '250-500ms'
				WHEN duration_ms < 1000 THEN '500ms-1s'
				WHEN duration_ms < 2500 THEN '1s-2.5s'
				WHEN duration_ms < 5000 THEN '2.5s-5s'
				ELSE '>5s'
			END as bucket_label,
			CASE
				WHEN duration_ms < 10 THEN 0
				WHEN duration_ms < 25 THEN 10
				WHEN duration_ms < 50 THEN 25
				WHEN duration_ms < 100 THEN 50
				WHEN duration_ms < 250 THEN 100
				WHEN duration_ms < 500 THEN 250
				WHEN duration_ms < 1000 THEN 500
				WHEN duration_ms < 2500 THEN 1000
				WHEN duration_ms < 5000 THEN 2500
				ELSE 5000
			END as bucket_min,
			COUNT(*) as span_count
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	if operationName != "" {
		query += ` AND operation_name = ?`
		args = append(args, operationName)
	}
	query += ` GROUP BY bucket_label, bucket_min ORDER BY bucket_min ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		min := dbutil.Int64FromAny(r["bucket_min"])
		r["bucket_max"] = min + 1
	}
	return rows, nil
}

func (r *Repository) GetLatencyHeatmap(teamUUID string, startMs, endMs int64, serviceName string) ([]map[string]any, error) {
	query := `
		SELECT toStartOfMinute(start_time) as time_bucket,
		       CASE
				WHEN duration_ms < 50 THEN '0-50ms'
				WHEN duration_ms < 100 THEN '50-100ms'
				WHEN duration_ms < 250 THEN '100-250ms'
				WHEN duration_ms < 500 THEN '250-500ms'
				WHEN duration_ms < 1000 THEN '500ms-1s'
				ELSE '>1s'
			END as latency_bucket,
			COUNT(*) as span_count
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY toStartOfMinute(start_time), latency_bucket
	           ORDER BY time_bucket ASC, latency_bucket ASC`

	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetSaturationMetrics(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       COUNT(*) as span_count,
		       AVG(duration_ms) as avg_duration_ms,
		       quantile(0.95)(duration_ms) as p95_duration_ms,
		       MAX(duration_ms) as max_duration_ms,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       avg(CAST(JSONExtractFloat(attributes, 'db.connection_pool.utilization') AS Float64)) as avg_db_pool_util,
		       max(CAST(JSONExtractFloat(attributes, 'db.connection_pool.utilization') AS Float64)) as max_db_pool_util,
		       avg(CAST(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') AS Float64)) as avg_consumer_lag,
		       max(CAST(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') AS Float64)) as max_consumer_lag,
		       avg(CAST(JSONExtractFloat(attributes, 'thread.pool.active') AS Float64)) as avg_thread_pool_active,
		       max(CAST(JSONExtractFloat(attributes, 'thread.pool.size') AS Float64)) as max_thread_pool_size,
		       avg(CAST(JSONExtractFloat(attributes, 'queue.depth') AS Float64)) as avg_queue_depth,
		       max(CAST(JSONExtractFloat(attributes, 'queue.depth') AS Float64)) as max_queue_depth
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY span_count DESC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetSaturationTimeSeries(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       toStartOfMinute(start_time) as timestamp,
		       COUNT(*) as span_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_duration_ms,
		       avg(CAST(JSONExtractFloat(attributes, 'db.connection_pool.utilization') AS Float64)) as avg_db_pool_util,
		       avg(CAST(JSONExtractFloat(attributes, 'messaging.kafka.consumer.lag') AS Float64)) as avg_consumer_lag,
		       avg(CAST(JSONExtractFloat(attributes, 'thread.pool.active') AS Float64)) as avg_thread_active,
		       avg(CAST(JSONExtractFloat(attributes, 'queue.depth') AS Float64)) as avg_queue_depth
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		GROUP BY service_name, toStartOfMinute(start_time)
		ORDER BY timestamp ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}
