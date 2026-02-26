package impl

import (
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/model"
)

// ClickHouseRepository encapsulates infrastructure data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new infrastructure repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetInfrastructure(teamUUID string, startMs, endMs int64) ([]model.InfrastructureSummary, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT if(host != '', host, ifNull(nullIf(JSONExtractString(attributes, 'host.name'), ''), 'unknown')) as host_name,
		       if(pod != '', pod, ifNull(nullIf(JSONExtractString(attributes, 'k8s.pod.name'), ''), '')) as pod_name,
		       if(container != '', container, ifNull(nullIf(JSONExtractString(attributes, 'k8s.container.name'), ''), '')) as container_name,
		       COUNT(*) as span_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       arrayStringConcat(groupUniqArray(if(service_name != '', service_name, 'unknown')), ',') as services_csv
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		GROUP BY host_name, pod_name, container_name
		ORDER BY span_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	summaries := make([]model.InfrastructureSummary, len(rows))
	for i, row := range rows {
		summaries[i] = model.InfrastructureSummary{
			Host:       dbutil.StringFromAny(row["host_name"]),
			Pod:        dbutil.StringFromAny(row["pod_name"]),
			Container:  dbutil.StringFromAny(row["container_name"]),
			SpanCount:  dbutil.Int64FromAny(row["span_count"]),
			ErrorCount: dbutil.Int64FromAny(row["error_count"]),
			AvgLatency: dbutil.Float64FromAny(row["avg_latency"]),
			P95Latency: dbutil.Float64FromAny(row["p95_latency"]),
			Services:   splitCSV(dbutil.StringFromAny(row["services_csv"])),
		}
	}
	return summaries, nil
}

func (r *ClickHouseRepository) GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]model.InfrastructureNode, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT if(host != '', host, ifNull(nullIf(JSONExtractString(attributes, 'host.name'), ''), 'unknown')) as host_name,
		       uniqExactIf(pod, pod != '') as pod_count,
		       uniqExactIf(container, container != '') as container_count,
		       arrayStringConcat(groupUniqArray(if(service_name != '', service_name, 'unknown')), ',') as services_csv,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(status='ERROR' OR http_status_code >= 400, 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       MAX(start_time) as last_seen
		FROM spans
		WHERE team_id = ? AND start_time BETWEEN ? AND ?
		GROUP BY host_name
		ORDER BY request_count DESC
		LIMIT 200
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	nodes := make([]model.InfrastructureNode, len(rows))
	for i, row := range rows {
		nodes[i] = model.InfrastructureNode{
			Host:           dbutil.StringFromAny(row["host_name"]),
			PodCount:       dbutil.Int64FromAny(row["pod_count"]),
			ContainerCount: dbutil.Int64FromAny(row["container_count"]),
			Services:       splitCSV(dbutil.StringFromAny(row["services_csv"])),
			RequestCount:   dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:     dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:      dbutil.Float64FromAny(row["error_rate"]),
			AvgLatencyMs:   dbutil.Float64FromAny(row["avg_latency"]),
			P95LatencyMs:   dbutil.Float64FromAny(row["p95_latency"]),
			LastSeen:       dbutil.StringFromAny(row["last_seen"]),
		}
	}
	return nodes, nil
}

func (r *ClickHouseRepository) GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]model.InfrastructureNodeService, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT if(service_name != '', service_name, 'unknown') as service_name,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(status='ERROR' OR http_status_code >= 400, 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(duration_ms) as avg_latency,
		       quantile(0.95)(duration_ms) as p95_latency,
		       uniqExact(pod) as pod_count
		FROM spans
		WHERE team_id = ?
		  AND if(host != '', host, ifNull(nullIf(JSONExtractString(attributes, 'host.name'), ''), 'unknown')) = ?
		  AND is_root = 1
		  AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
		LIMIT 100
	`, teamUUID, host, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	services := make([]model.InfrastructureNodeService, len(rows))
	for i, row := range rows {
		services[i] = model.InfrastructureNodeService{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:    dbutil.Float64FromAny(row["error_rate"]),
			AvgLatencyMs: dbutil.Float64FromAny(row["avg_latency"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency"]),
			PodCount:     dbutil.Int64FromAny(row["pod_count"]),
		}
	}
	return services, nil
}

func splitCSV(s string) []string {
	// ClickHouse groupArray returns values like ['item1','item2']
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	parts := strings.Split(s, ",")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "'\"")
		if p != "" {
			clean = append(clean, p)
		}
	}
	return clean
}
