package topology

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Health thresholds for node coloring (fractions, not percentages).
const (
	unhealthyErrorRate = 0.05
	degradedErrorRate  = 0.01
)

// Service orchestrates topology construction.
type Service interface {
	GetTopology(ctx context.Context, teamID int64, startMs, endMs int64, focusService string) (TopologyResponse, error)
}

type topologyService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &topologyService{repo: repo}
}

func (s *topologyService) GetTopology(ctx context.Context, teamID int64, startMs, endMs int64, focusService string) (TopologyResponse, error) {
	var (
		nodeRows []nodeAggRow
		edgeRows []edgeAggRow
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rows, err := s.repo.GetNodes(gctx, teamID, startMs, endMs)
		if err != nil {
			return err
		}
		nodeRows = rows
		return nil
	})
	g.Go(func() error {
		rows, err := s.repo.GetEdges(gctx, teamID, startMs, endMs)
		if err != nil {
			return err
		}
		edgeRows = rows
		return nil
	})
	if err := g.Wait(); err != nil {
		return TopologyResponse{}, err
	}

	nodes := buildNodes(nodeRows)
	edges := buildEdges(edgeRows)

	// Ensure every edge endpoint has a node, even if it has no inbound spans
	// (e.g. a pure CLIENT-only producer).
	nodes = addMissingEdgeNodes(nodes, edges)

	if focusService != "" {
		nodes, edges = filterNeighborhood(nodes, edges, focusService)
	}

	return TopologyResponse{Nodes: nodes, Edges: edges}, nil
}

func buildNodes(rows []nodeAggRow) []ServiceNode {
	out := make([]ServiceNode, 0, len(rows))
	for _, r := range rows {
		var errRate float64
		if r.RequestCount > 0 {
			errRate = float64(r.ErrorCount) / float64(r.RequestCount)
		}
		out = append(out, ServiceNode{
			Name:         r.ServiceName,
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

func buildEdges(rows []edgeAggRow) []ServiceEdge {
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

// filterNeighborhood keeps only the focus service and its direct neighbors
// (1-hop, both upstream and downstream).
func filterNeighborhood(nodes []ServiceNode, edges []ServiceEdge, focus string) ([]ServiceNode, []ServiceEdge) {
	keep := map[string]struct{}{focus: {}}
	keepEdges := make([]ServiceEdge, 0, len(edges))
	for _, e := range edges {
		if e.Source == focus || e.Target == focus {
			keepEdges = append(keepEdges, e)
			keep[e.Source] = struct{}{}
			keep[e.Target] = struct{}{}
		}
	}
	keepNodes := make([]ServiceNode, 0, len(keep))
	for _, n := range nodes {
		if _, ok := keep[n.Name]; ok {
			keepNodes = append(keepNodes, n)
		}
	}
	return keepNodes, keepEdges
}
