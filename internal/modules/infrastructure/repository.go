package infrastructure

import dbutil "github.com/observability/observability-backend-go/internal/database"

// Repository encapsulates infrastructure data access logic.
type Repository struct {
	db dbutil.Querier
}

// NewRepository creates a new infrastructure repository.
func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
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
