package topology

import (
	"context"
	"fmt"

	"github.com/Optikk-Org/optikk-backend/internal/infra/sketch"
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
	repo    Repository
	sketchQ *sketch.Querier
}

func NewService(repo Repository, sketchQ *sketch.Querier) Service {
	return &topologyService{repo: repo, sketchQ: sketchQ}
}

// teamIDString converts the int64 tenant id to the string form used by all
// sketch keys.
func teamIDString(teamID int64) string { return fmt.Sprintf("%d", teamID) }

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

	// Attach percentiles from sketch.Querier. Nodes key by service name
	// (SpanLatencyService). Edges key by the callee (child) service — the CH
	// query groups by (source, target) using child-span latencies, so the
	// per-target sketch is the correct aggregate to read for the edge p50/p95.
	if s.sketchQ != nil && (len(nodeRows) > 0 || len(edgeRows) > 0) {
		pcts, _ := s.sketchQ.Percentiles(ctx, sketch.SpanLatencyService, teamIDString(teamID), startMs, endMs, 0.5, 0.95, 0.99)
		for i := range nodeRows {
			dim := sketch.DimSpanService(nodeRows[i].ServiceName)
			if v, ok := pcts[dim]; ok && len(v) == 3 {
				nodeRows[i].P50Ms = v[0]
				nodeRows[i].P95Ms = v[1]
				nodeRows[i].P99Ms = v[2]
			}
		}
		for i := range edgeRows {
			dim := sketch.DimSpanService(edgeRows[i].Target)
			if v, ok := pcts[dim]; ok && len(v) >= 2 {
				edgeRows[i].P50Ms = v[0]
				edgeRows[i].P95Ms = v[1]
			}
		}
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
