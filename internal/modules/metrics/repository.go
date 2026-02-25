package metrics

import (
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

func (r *Repository) GetDashboardOverview(teamUUID string, start, end time.Time) ([]map[string]any, []map[string]any, []map[string]any, error) {
	serviceMetrics, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name
	`, teamUUID, start, end)
	if err != nil {
		return nil, nil, nil, err
	}

	logsData, err := dbutil.QueryMaps(r.db, `
		SELECT timestamp, level, service_name, message, trace_id, span_id
		FROM logs
		WHERE team_id = ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC
		LIMIT 10
	`, teamUUID, start, end)
	if err != nil {
		return nil, nil, nil, err
	}

	tracesData, err := dbutil.QueryMaps(r.db, `
		SELECT trace_id, service_name, operation_name, start_time, duration_ms, status
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		ORDER BY start_time DESC
		LIMIT 10
	`, teamUUID, start, end)

	return serviceMetrics, logsData, tracesData, err
}

func (r *Repository) GetDashboardServices(teamUUID string, start, end time.Time) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
	`, teamUUID, start, end)
}

func (r *Repository) GetDashboardServiceDetail(teamUUID, serviceName string, start, end time.Time) (map[string]any, error) {
	return dbutil.QueryMap(r.db, `
		SELECT service_name,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND service_name = ? AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		LIMIT 1
	`, teamUUID, serviceName, start, end)
}

func (r *Repository) GetServiceMetrics(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       quantile(0.99)(duration_ms) as p99_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetEndpointMetrics(teamUUID string, startMs, endMs int64, serviceName string) ([]map[string]any, error) {
	query := `
		SELECT service_name, operation_name, http_method,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       quantile(0.99)(duration_ms) as p99_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, operation_name, http_method ORDER BY request_count DESC LIMIT 100`
	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetMetricsTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]map[string]any, error) {
	query := `
		SELECT toStartOfMinute(start_time) as time_bucket,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50,
		       quantile(0.95)(duration_ms) as p95,
		       quantile(0.99)(duration_ms) as p99
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY time_bucket ORDER BY time_bucket ASC`
	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetMetricsSummary(teamUUID string, startMs, endMs int64) (map[string]any, error) {
	return dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_requests,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       if(COUNT(*)>0, sum(if(status='ERROR', 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       quantile(0.99)(duration_ms) as p99_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetServiceTimeSeries(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       toStartOfMinute(start_time) as timestamp,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name, toStartOfMinute(start_time)
		ORDER BY timestamp ASC, request_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetServiceTopologyNodes(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name, COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetServiceTopologyEdges(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT p.service_name as source,
		       c.service_name as target,
		       COUNT(*) as call_count,
		       AVG(c.duration_ms) as avg_latency,
		       if(COUNT(*) > 0, sum(if(c.status='ERROR', 1, 0))*100.0/COUNT(*), 0) as error_rate
		FROM spans c
		JOIN spans p ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id AND c.team_id = p.team_id
		WHERE c.team_id = ? AND c.start_time BETWEEN ? AND ? AND p.service_name != c.service_name
		GROUP BY p.service_name, c.service_name
		ORDER BY call_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetInfrastructure(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT host, pod, container,
		       COUNT(*) as span_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       groupArray(DISTINCT service_name) as services_csv
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ? AND host != ''
		GROUP BY host, pod, container
		ORDER BY span_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT host,
		       uniqExact(pod) as pod_count,
		       uniqExact(container) as container_count,
		       groupArray(DISTINCT service_name) as services_csv,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(status='ERROR', 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       MAX(start_time) as last_seen
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ? AND host != ''
		GROUP BY host
		ORDER BY request_count DESC
		LIMIT 200
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

func (r *Repository) GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(status='ERROR', 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       uniqExact(pod) as pod_count
		FROM spans
		WHERE team_id = ? AND host = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
		LIMIT 100
	`, teamUUID, host, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

// GetEndpointTimeSeries retrieves timeseries for each individual endpoint/operation
func (r *Repository) GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]map[string]any, error) {
	query := `
		SELECT toStartOfMinute(start_time) as time_bucket,
		       service_name,
		       operation_name,
		       http_method,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR', 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.5)(duration_ms) as p50,
		       quantile(0.95)(duration_ms) as p95,
		       quantile(0.99)(duration_ms) as p99
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY time_bucket, service_name, operation_name, http_method ORDER BY time_bucket ASC`
	return dbutil.QueryMaps(r.db, query, args...)
}
