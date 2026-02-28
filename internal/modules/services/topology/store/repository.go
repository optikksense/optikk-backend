package store

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/services/topology/model"
)

// Repository encapsulates data access logic for the services topology page.
type Repository interface {
	GetTopology(teamUUID string, startMs, endMs int64) (model.TopologyData, error)
}

// ClickHouseRepository encapsulates services topology data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new services topology repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTopology(teamUUID string, startMs, endMs int64) (model.TopologyData, error) {
	nodesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       COUNT(*) as request_count,
		       sum(if(status='ERROR' OR http_status_code >= 400, 1, 0)) as error_count,
		       AVG(duration_ms) as avg_latency
		FROM spans
		WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY request_count DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.TopologyData{}, err
	}

	edgesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT p.service_name as source,
		       c.service_name as target,
		       COUNT(*) as call_count,
		       AVG(c.duration_ms) as avg_latency,
		       if(COUNT(*) > 0, sum(if(c.status='ERROR' OR c.http_status_code >= 400, 1, 0))*100.0/COUNT(*), 0) as error_rate
		FROM spans c
		JOIN spans p ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id AND c.team_id = p.team_id
		WHERE c.team_id = ? AND c.start_time BETWEEN ? AND ? AND p.service_name != c.service_name
		GROUP BY p.service_name, c.service_name
		ORDER BY call_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.TopologyData{}, err
	}

	nodes := make([]model.TopologyNode, len(nodesRaw))
	for i, row := range nodesRaw {
		requestCount := dbutil.Int64FromAny(row["request_count"])
		errorCount := dbutil.Int64FromAny(row["error_count"])
		errorRate := 0.0
		if requestCount > 0 {
			errorRate = float64(errorCount) * 100.0 / float64(requestCount)
		}

		nodes[i] = model.TopologyNode{
			Name:         dbutil.StringFromAny(row["service_name"]),
			Status:       serviceStatus(errorRate),
			RequestCount: requestCount,
			ErrorRate:    errorRate,
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		}
	}

	edges := make([]model.TopologyEdge, len(edgesRaw))
	for i, row := range edgesRaw {
		edges[i] = model.TopologyEdge{
			Source:     dbutil.StringFromAny(row["source"]),
			Target:     dbutil.StringFromAny(row["target"]),
			CallCount:  dbutil.Int64FromAny(row["call_count"]),
			AvgLatency: dbutil.Float64FromAny(row["avg_latency"]),
			ErrorRate:  dbutil.Float64FromAny(row["error_rate"]),
		}
	}

	return model.TopologyData{
		Nodes: nodes,
		Edges: edges,
	}, nil
}

func serviceStatus(errorRate float64) string {
	if errorRate > 5 {
		return "unhealthy"
	}
	if errorRate > 1 {
		return "degraded"
	}
	return "healthy"
}
