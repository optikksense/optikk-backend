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
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY countMerge(request_count) DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return model.TopologyData{}, err
	}

	// Reads spans_edges_1m — no self-JOIN on raw spans.
	edgesRaw, err := dbutil.QueryMaps(r.db, `
		SELECT parent_service_name                                              AS source,
		       service_name                                                     AS target,
		       countMerge(call_count)                                          AS call_count,
		       avgMerge(avg_latency_state)                                     AS avg_latency,
		       if(countMerge(call_count) > 0,
		          countMerge(error_count)*100.0/countMerge(call_count), 0)    AS error_rate
		FROM observability.spans_edges_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY parent_service_name, service_name
		ORDER BY countMerge(call_count) DESC
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
