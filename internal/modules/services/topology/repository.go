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
	// Nodes: one row per service — root spans only, aggregated from spans+resources JOIN.
	nodesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, request_count, error_count, avg_latency
		FROM (
			SELECT r.service_name,
			       count()                          AS request_count,
			       countIf(`+ErrorCondition()+`)    AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.timestamp BETWEEN ? AND ?
			GROUP BY r.service_name
		)
		ORDER BY request_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return TopologyData{}, err
	}

	// Edges: service-to-service calls derived from client spans (kind=3) joined to their
	// parent spans across the same trace. r1=caller service, r2=callee service.
	edgesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT source,
		       target,
		       call_count,
		       avg_latency,
		       p95_latency_ms,
		       if(call_count > 0, error_count*100.0/call_count, 0) AS error_rate
		FROM (
			SELECT r1.service_name                              AS source,
			       r2.service_name                              AS target,
			       count()                                      AS call_count,
			       countIf(s1.has_error = true OR s1.response_status_code >= '400') AS error_count,
			       avg(s1.duration_nano / 1000000.0)            AS avg_latency,
			       quantile(0.95)(s1.duration_nano / 1000000.0) AS p95_latency_ms
			FROM observability.spans s1
			ANY INNER JOIN observability.resources r1 ON s1.team_id = r1.team_id AND s1.resource_fingerprint = r1.fingerprint
			JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id
			ANY INNER JOIN observability.resources r2 ON s2.team_id = r2.team_id AND s2.resource_fingerprint = r2.fingerprint
			WHERE s1.team_id = ? AND s1.kind = 3 AND s1.timestamp BETWEEN ? AND ?
			  AND r1.service_name != r2.service_name
			GROUP BY r1.service_name, r2.service_name
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
