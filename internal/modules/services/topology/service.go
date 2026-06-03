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
	GetTopology(ctx context.Context, teamID, startMs, endMs int64, focusService string) (TopologyResponse, error)
}

type topologyService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &topologyService{repo: repo}
}

func (s *topologyService) GetTopology(ctx context.Context, teamID, startMs, endMs int64, focusService string) (TopologyResponse, error) {
	var (
		nodeRows []nodeAggRow
		edgeRows []edgeAggRow
	)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		rows, err := s.repo.GetNodes(gctx, teamID, startMs, endMs, focusService)
		if err != nil {
			return err
		}
		nodeRows = rows
		return nil
	})
	g.Go(func() error {
		rows, err := s.repo.GetEdges(gctx, teamID, startMs, endMs, focusService)
		if err != nil {
			return err
		}
		edgeRows = rows
		return nil
	})
	if err := g.Wait(); err != nil {
		return TopologyResponse{}, err
	}

	graph := BuildGraph(nodeAggsFromRows(nodeRows), edgeAggsFromRows(edgeRows))
	nodes, edges := graph.Nodes, graph.Edges

	if focusService != "" {
		nodes, edges = filterNeighborhood(nodes, edges, focusService)
	}

	return TopologyResponse{Nodes: nodes, Edges: edges}, nil
}

// nodeAggsFromRows maps the rollup query rows onto the neutral BuildGraph input.
func nodeAggsFromRows(rows []nodeAggRow) []NodeAgg {
	out := make([]NodeAgg, len(rows))
	for i, r := range rows {
		out[i] = NodeAgg{
			Service:      r.ServiceName,
			RequestCount: int64(r.RequestCount), //nolint:gosec // domain-bounded
			ErrorCount:   int64(r.ErrorCount),   //nolint:gosec // domain-bounded
			P50Ms:        float64(r.P50Ms),
			P95Ms:        float64(r.P95Ms),
			P99Ms:        float64(r.P99Ms),
		}
	}
	return out
}

func edgeAggsFromRows(rows []edgeAggRow) []EdgeAgg {
	out := make([]EdgeAgg, len(rows))
	for i, r := range rows {
		out[i] = EdgeAgg{
			Source:     r.Source,
			Target:     r.Target,
			CallCount:  int64(r.CallCount),  //nolint:gosec // domain-bounded
			ErrorCount: int64(r.ErrorCount), //nolint:gosec // domain-bounded
			P50Ms:      float64(r.P50Ms),
			P95Ms:      float64(r.P95Ms),
		}
	}
	return out
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
