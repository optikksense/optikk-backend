package topology

// NodeAgg is the source-agnostic per-service aggregate that feeds BuildGraph.
type NodeAgg struct {
	Service      string
	RequestCount int64
	ErrorCount   int64
	P50Ms        float64
	P95Ms        float64
	P99Ms        float64
}

// EdgeAgg is the source-agnostic directed-call aggregate for BuildGraph.
type EdgeAgg struct {
	Source     string
	Target     string
	CallCount  int64
	ErrorCount int64
	P50Ms      float64
	P95Ms      float64
}

// BuildGraph derives the node+edge graph from neutral aggregates.
func BuildGraph(nodeAggs []NodeAgg, edgeAggs []EdgeAgg) TopologyResponse {
	nodes := buildNodes(nodeAggs)
	edges := buildEdges(edgeAggs)
	nodes = addMissingEdgeNodes(nodes, edges)
	return TopologyResponse{Nodes: nodes, Edges: edges}
}

func buildNodes(rows []NodeAgg) []ServiceNode {
	out := make([]ServiceNode, 0, len(rows))
	for _, r := range rows {
		var errRate float64
		if r.RequestCount > 0 {
			errRate = float64(r.ErrorCount) / float64(r.RequestCount)
		}
		out = append(out, ServiceNode{
			Name:         r.Service,
			RequestCount: r.RequestCount,
			ErrorCount:   r.ErrorCount,
			ErrorRate:    errRate,
			P50LatencyMs: r.P50Ms,
			P95LatencyMs: r.P95Ms,
			P99LatencyMs: r.P99Ms,
			Health:       classifyHealth(errRate),
		})
	}
	return out
}

func buildEdges(rows []EdgeAgg) []ServiceEdge {
	out := make([]ServiceEdge, 0, len(rows))
	for _, r := range rows {
		var errRate float64
		if r.CallCount > 0 {
			errRate = float64(r.ErrorCount) / float64(r.CallCount)
		}
		out = append(out, ServiceEdge{
			Source:       r.Source,
			Target:       r.Target,
			CallCount:    r.CallCount,
			ErrorCount:   r.ErrorCount,
			ErrorRate:    errRate,
			P50LatencyMs: r.P50Ms,
			P95LatencyMs: r.P95Ms,
		})
	}
	return out
}

func classifyHealth(errRate float64) string {
	switch {
	case errRate > unhealthyErrorRate:
		return HealthUnhealthy
	case errRate > degradedErrorRate:
		return HealthDegraded
	default:
		return HealthHealthy
	}
}

func addMissingEdgeNodes(nodes []ServiceNode, edges []ServiceEdge) []ServiceNode {
	index := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		index[n.Name] = struct{}{}
	}
	for _, e := range edges {
		for _, name := range []string{e.Source, e.Target} {
			if _, ok := index[name]; ok {
				continue
			}
			nodes = append(nodes, ServiceNode{Name: name, Health: HealthHealthy})
			index[name] = struct{}{}
		}
	}
	return nodes
}
