package nodes

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for infrastructure node tracking.
type Repository interface {
	GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]InfrastructureNode, error)
	GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]InfrastructureNodeService, error)
}

// ClickHouseRepository encapsulates infrastructure node data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new node repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]InfrastructureNode, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT `+HostNameExpression()+` as host_name,
		       uniqExactIf(`+ColPod+`, `+ColPod+` != '') as pod_count,
		       uniqExactIf(`+ColContainer+`, `+ColContainer+` != '') as container_count,
		       arrayStringConcat(groupUniqArray(if(`+ColServiceName+` != '', `+ColServiceName+`, '`+DefaultUnknown+`')), ',') as services_csv,
		       COUNT(*) as request_count,
		       sum(if(`+ErrorCondition()+`, 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(`+ErrorCondition()+`, 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(`+ColDurationMs+`) as avg_latency,
		       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(`+ColDurationMs+`) as p95_latency,
		       MAX(`+ColStartTime+`) as last_seen
		FROM spans
		WHERE `+ColTeamID+` = ? AND `+ColStartTime+` BETWEEN ? AND ?
		GROUP BY host_name
		ORDER BY request_count DESC
		LIMIT `+fmt.Sprintf("%d", MaxNodes)+`
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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

func (r *ClickHouseRepository) GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT if(`+ColServiceName+` != '', `+ColServiceName+`, '`+DefaultUnknown+`') as service_name,
		       COUNT(*) as request_count,
		       sum(if(`+ErrorCondition()+`, 1, 0)) as error_count,
		       if(COUNT(*) > 0, sum(if(`+ErrorCondition()+`, 1, 0))*100.0/COUNT(*), 0) as error_rate,
		       AVG(`+ColDurationMs+`) as avg_latency,
		       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(`+ColDurationMs+`) as p95_latency,
		       uniqExact(`+ColPod+`) as pod_count
		FROM spans
		WHERE `+ColTeamID+` = ?
		  AND `+HostNameExpression()+` = ?
		  AND `+RootSpanCondition()+`
		  AND `+ColStartTime+` BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
		LIMIT `+fmt.Sprintf("%d", MaxServices)+`
	`, teamUUID, host, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

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
