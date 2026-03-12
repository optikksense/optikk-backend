package nodes

import (
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository interface {
	GetInfrastructureNodes(teamID int64, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeServices(teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetInfrastructureNodes(teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	const rootSpanCondition = "(s.parent_span_id = '' OR s.parent_span_id = '0000000000000000')"
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT if(s.mat_host_name != '', s.mat_host_name, '`+DefaultUnknown+`') as host_name,
		       uniqExactIf(s.mat_k8s_pod_name, s.mat_k8s_pod_name != '') as pod_count,
		       toInt64(0) as container_count,
		       arrayStringConcat(groupUniqArray(s.service_name), ',') as services_csv,
		       COUNT(*) as request_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) as error_count,
		       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
		       AVG(s.duration_nano / 1000000.0) as avg_latency,
		       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) as p95_latency,
		       MAX(s.timestamp) as last_seen
		FROM observability.spans s
		WHERE s.team_id = ? AND `+rootSpanCondition+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		GROUP BY host_name
		ORDER BY request_count DESC
		LIMIT `+fmt.Sprintf("%d", MaxNodes)+`
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	nodes := make([]InfrastructureNode, len(rows))
	for i, row := range rows {
		nodes[i] = InfrastructureNode{
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

func (r *ClickHouseRepository) GetInfrastructureNodeServices(teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	const rootSpanCondition = "(s.parent_span_id = '' OR s.parent_span_id = '0000000000000000')"
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.service_name as service_name,
		       COUNT(*) as request_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) as error_count,
		       if(COUNT(*) > 0, countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)*100.0/COUNT(*), 0) as error_rate,
		       AVG(s.duration_nano / 1000000.0) as avg_latency,
		       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) as p95_latency,
		       uniqExactIf(s.mat_k8s_pod_name, s.mat_k8s_pod_name != '') as pod_count
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND if(s.mat_host_name != '', s.mat_host_name, '`+DefaultUnknown+`') = ?
		  AND `+rootSpanCondition+`
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
		LIMIT `+fmt.Sprintf("%d", MaxServices)+`
	`, teamID, host, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	services := make([]InfrastructureNodeService, len(rows))
	for i, row := range rows {
		services[i] = InfrastructureNodeService{
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
