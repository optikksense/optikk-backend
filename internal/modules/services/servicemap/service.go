package servicemap

import (
	"context"
	"fmt"
	"strings"
)

type Service interface {
	GetTopology(teamID int64, startMs, endMs int64) (TopologyData, error)
	GetUpstreamDownstream(teamID int64, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error)
	GetServiceDependencyGraph(teamID int64, serviceName string, startMs, endMs int64) (ServiceDependencyGraph, error)
	GetEnrichedTopology(teamID int64, startMs, endMs int64) (EnrichedTopologyData, error)
	GetTopologyClusters(teamID int64, startMs, endMs int64) ([]TopologyCluster, error)
	GetExternalDependencies(teamID int64, startMs, endMs int64) ([]ExternalDependency, error)
	GetClientServerLatency(teamID int64, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error)
}

type ServiceMapService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &ServiceMapService{repo: repo}
}

func (s *ServiceMapService) GetTopology(teamID int64, startMs, endMs int64) (TopologyData, error) {
	ctx := context.Background()

	nodeRows, err := s.repo.GetTopologyNodes(ctx, teamID, startMs, endMs)
	if err != nil {
		return TopologyData{}, fmt.Errorf("servicemap.GetTopologyNodes: %w", err)
	}

	edges, err := s.repo.GetTopologyEdges(ctx, teamID, startMs, endMs)
	if err != nil {
		return TopologyData{}, fmt.Errorf("servicemap.GetTopologyEdges: %w", err)
	}

	nodes := make([]TopologyNode, len(nodeRows))
	for i, r := range nodeRows {
		errorRate := 0.0
		if r.RequestCount > 0 {
			errorRate = float64(r.ErrorCount) * 100.0 / float64(r.RequestCount)
		}

		nodes[i] = TopologyNode{
			Name:         r.Name,
			Status:       serviceStatus(errorRate),
			RequestCount: r.RequestCount,
			ErrorRate:    errorRate,
			AvgLatency:   r.AvgLatency,
		}
	}

	return TopologyData{Nodes: nodes, Edges: edges}, nil
}

func (s *ServiceMapService) GetUpstreamDownstream(teamID int64, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error) {
	rows, err := s.repo.GetUpstreamDownstream(context.Background(), teamID, serviceName, startMs, endMs)
	if err != nil {
		return nil, err
	}
	result := make([]ServiceDependencyDetail, len(rows))
	for i, r := range rows {
		direction := "downstream"
		if r.Target == serviceName {
			direction = "upstream"
		}
		result[i] = ServiceDependencyDetail{
			Source:       r.Source,
			Target:       r.Target,
			CallCount:    r.CallCount,
			P95LatencyMs: r.P95LatencyMs,
			ErrorRate:    r.ErrorRate,
			Direction:    direction,
		}
	}
	return result, nil
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

func (s *ServiceMapService) GetServiceDependencyGraph(teamID int64, serviceName string, startMs, endMs int64) (ServiceDependencyGraph, error) {
	ctx := context.Background()

	// Get edges involving this service
	edgeRows, err := s.repo.GetUpstreamDownstream(ctx, teamID, serviceName, startMs, endMs)
	if err != nil {
		return ServiceDependencyGraph{}, fmt.Errorf("servicemap.GetServiceDependencyGraph edges: %w", err)
	}

	// Collect unique connected service names
	connectedNames := make(map[string]struct{})
	connectedNames[serviceName] = struct{}{}
	edges := make([]TopologyEdge, len(edgeRows))
	for i, r := range edgeRows {
		connectedNames[r.Source] = struct{}{}
		connectedNames[r.Target] = struct{}{}
		edges[i] = TopologyEdge{
			Source:       r.Source,
			Target:       r.Target,
			CallCount:    r.CallCount,
			AvgLatency:   0, // not available from this query
			P95LatencyMs: r.P95LatencyMs,
			ErrorRate:    r.ErrorRate,
		}
	}

	// Get node-level metrics for all connected services
	allNodes, err := s.repo.GetTopologyNodes(ctx, teamID, startMs, endMs)
	if err != nil {
		return ServiceDependencyGraph{}, fmt.Errorf("servicemap.GetServiceDependencyGraph nodes: %w", err)
	}

	var nodes []TopologyNode
	for _, r := range allNodes {
		if _, ok := connectedNames[r.Name]; !ok {
			continue
		}
		errorRate := 0.0
		if r.RequestCount > 0 {
			errorRate = float64(r.ErrorCount) * 100.0 / float64(r.RequestCount)
		}
		nodes = append(nodes, TopologyNode{
			Name:         r.Name,
			Status:       serviceStatus(errorRate),
			RequestCount: r.RequestCount,
			ErrorRate:    errorRate,
			AvgLatency:   r.AvgLatency,
		})
	}

	return ServiceDependencyGraph{
		Center: serviceName,
		Nodes:  nodes,
		Edges:  edges,
	}, nil
}

func (s *ServiceMapService) GetEnrichedTopology(teamID int64, startMs, endMs int64) (EnrichedTopologyData, error) {
	topo, err := s.GetTopology(teamID, startMs, endMs)
	if err != nil {
		return EnrichedTopologyData{}, fmt.Errorf("servicemap.GetEnrichedTopology: %w", err)
	}

	enriched := make([]EnrichedTopologyNode, len(topo.Nodes))
	for i, n := range topo.Nodes {
		enriched[i] = EnrichedTopologyNode{
			Name:         n.Name,
			Status:       n.Status,
			RequestCount: n.RequestCount,
			ErrorRate:    n.ErrorRate,
			AvgLatency:   n.AvgLatency,
			ServiceType:  inferServiceType(n.Name),
			Sparkline:    []int64{}, // placeholder — real sparkline requires per-node timeseries query
		}
	}

	return EnrichedTopologyData{Nodes: enriched, Edges: topo.Edges}, nil
}

func (s *ServiceMapService) GetTopologyClusters(teamID int64, startMs, endMs int64) ([]TopologyCluster, error) {
	topo, err := s.GetTopology(teamID, startMs, endMs)
	if err != nil {
		return nil, fmt.Errorf("servicemap.GetTopologyClusters: %w", err)
	}

	// Group by common prefix (split on - or .)
	prefixMap := make(map[string][]string)
	for _, n := range topo.Nodes {
		parts := splitServiceName(n.Name)
		if len(parts) >= 2 {
			prefix := parts[0]
			prefixMap[prefix] = append(prefixMap[prefix], n.Name)
		}
	}

	var clusters []TopologyCluster
	for name, services := range prefixMap {
		if len(services) >= 2 {
			clusters = append(clusters, TopologyCluster{
				Name:     name,
				Services: services,
				Count:    len(services),
			})
		}
	}
	return clusters, nil
}

func inferServiceType(name string) string {
	lower := strings.ToLower(name)
	switch {
	case containsAny(lower, "postgres", "mysql", "maria", "mongo", "clickhouse", "sqlite", "cockroach", "database"):
		return "database"
	case containsAny(lower, "redis", "memcache", "elasticache", "valkey", "cache"):
		return "cache"
	case containsAny(lower, "kafka", "rabbit", "nats", "sqs", "pulsar", "amqp", "queue"):
		return "queue"
	case containsAny(lower, "grpc"):
		return "grpc"
	default:
		return "application"
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func splitServiceName(name string) []string {
	// Split on - . or _
	var parts []string
	current := ""
	for _, c := range name {
		if c == '-' || c == '.' || c == '_' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func (s *ServiceMapService) GetExternalDependencies(teamID int64, startMs, endMs int64) ([]ExternalDependency, error) {
	return s.repo.GetExternalDependencies(context.Background(), teamID, startMs, endMs)
}

func (s *ServiceMapService) GetClientServerLatency(teamID int64, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error) {
	rows, err := s.repo.GetClientServerLatency(context.Background(), teamID, startMs, endMs, operationName)
	if err != nil {
		return nil, err
	}
	result := make([]ClientServerLatencyPoint, len(rows))
	for i, r := range rows {
		result[i] = ClientServerLatencyPoint{
			Timestamp:     r.Timestamp,
			OperationName: r.OperationName,
			ClientP95Ms:   r.ClientP95Ms,
			ServerP95Ms:   r.ServerP95Ms,
			NetworkGapMs:  r.ClientP95Ms - r.ServerP95Ms,
		}
	}
	return result, nil
}
