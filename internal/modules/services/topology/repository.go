package topology

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for the services topology page.
type Repository interface {
	GetTopology(teamUUID string, startMs, endMs int64) (TopologyData, error)
}

// ClickHouseRepository encapsulates services topology data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new services topology repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTopology(teamUUID string, startMs, endMs int64) (TopologyData, error) {
	nodesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, request_count, error_count, avg_latency
		FROM (
			SELECT `+ColServiceName+`,
			       count()                   AS request_count,
			       countIf(`+ErrorCondition()+`) AS error_count,
			       avg(`+ColDurationMs+`)          AS avg_latency
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?
			GROUP BY `+ColServiceName+`
		)
		ORDER BY request_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return TopologyData{}, err
	}

	// Reads service-to-service edges from spans
	edgesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT source,
		       target,
		       call_count,
		       avg_latency,
		       p95_latency_ms,
		       if(call_count > 0, error_count*100.0/call_count, 0) AS error_rate
		FROM (
			SELECT `+ColParentServiceName+`                    AS source,
			       `+ColServiceName+`                           AS target,
			       count()                               AS call_count,
			       countIf(`+ErrorCondition()+`)         AS error_count,
			       avg(`+ColDurationMs+`)                AS avg_latency,
			       quantile(0.95)(`+ColDurationMs+`)     AS p95_latency_ms
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+ColParentServiceName+` != '' AND `+ColStartTime+` BETWEEN ? AND ?
			GROUP BY `+ColParentServiceName+`, `+ColServiceName+`
		)
		ORDER BY call_count DESC
		LIMIT `+fmt.Sprintf("%d", MaxEdges)+`
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return TopologyData{}, err
	}

	nodes := make([]TopologyNode, len(nodesRaw))
	for i, row := range nodesRaw {
		requestCount := dbutil.Int64FromAny(row["request_count"])
		errorCount := dbutil.Int64FromAny(row["error_count"])
		errorRate := 0.0
		if requestCount > 0 {
			errorRate = float64(errorCount) * 100.0 / float64(requestCount)
		}

		nodes[i] = TopologyNode{
			Name:         dbutil.StringFromAny(row["service_name"]),
			Status:       serviceStatus(errorRate),
			RequestCount: requestCount,
			ErrorRate:    errorRate,
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		}
	}

	edges := make([]TopologyEdge, len(edgesRaw))
	for i, row := range edgesRaw {
		edges[i] = TopologyEdge{
			Source:       dbutil.StringFromAny(row["source"]),
			Target:       dbutil.StringFromAny(row["target"]),
			CallCount:    dbutil.Int64FromAny(row["call_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
			ErrorRate:    dbutil.Float64FromAny(row["error_rate"]),
		}
	}

	return TopologyData{
		Nodes: nodes,
		Edges: edges,
	}, nil
}

func serviceStatus(errorRate float64) string {
	if errorRate > UnhealthyMinErrorRate {
		return StatusUnhealthy
	}
	if errorRate > HealthyMaxErrorRate {
		return StatusDegraded
	}
	return StatusHealthy
}
