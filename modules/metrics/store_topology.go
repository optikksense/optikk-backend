package metrics

import (
	"context"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// GetTopologyNodes builds node list from service metrics — no separate query.
func (r *ClickHouseRepository) GetTopologyNodes(ctx context.Context, f MetricFilters) ([]TopologyNode, error) {
	services, err := r.GetServiceMetrics(ctx, f)
	if err != nil {
		return nil, err
	}

	nodes := make([]TopologyNode, 0, len(services))
	for _, s := range services {
		nodes = append(nodes, TopologyNode{
			Name:         s.ServiceName,
			Status:       s.Status,
			RequestCount: s.RequestCount,
			ErrorRate:    s.ErrorRate,
			AvgLatency:   s.AvgLatency,
		})
	}
	return nodes, nil
}

// GetTopologyEdges returns service-to-service edges from the pre-aggregated view.
func (r *ClickHouseRepository) GetTopologyEdges(ctx context.Context, f MetricFilters) ([]TopologyEdge, error) {
	rows, err := dbutil.QueryMaps(r.db, `
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
	`, f.TeamUUID, f.Start, f.End)
	if err != nil {
		return nil, err
	}

	edges := make([]TopologyEdge, 0, len(rows))
	for _, row := range rows {
		edges = append(edges, TopologyEdge{
			Source:     dbutil.StringFromAny(row["source"]),
			Target:     dbutil.StringFromAny(row["target"]),
			CallCount:  dbutil.Int64FromAny(row["call_count"]),
			AvgLatency: dbutil.Float64FromAny(row["avg_latency"]),
			ErrorRate:  dbutil.Float64FromAny(row["error_rate"]),
		})
	}
	return edges, nil
}

// GetTopology returns the complete topology graph (nodes + edges).
func (r *ClickHouseRepository) GetTopology(ctx context.Context, f MetricFilters) (TopologyData, error) {
	nodes, err := r.GetTopologyNodes(ctx, f)
	if err != nil {
		return TopologyData{}, err
	}
	edges, err := r.GetTopologyEdges(ctx, f)
	if err != nil {
		return TopologyData{}, err
	}
	return TopologyData{Nodes: nodes, Edges: edges}, nil
}
