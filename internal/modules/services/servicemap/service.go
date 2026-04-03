package servicemap

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	serviceinventory "github.com/Optikk-Org/optikk-backend/internal/modules/services/inventory"
)

const statusUnknown = "unknown"

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
	repo          Repository
	inventoryRepo serviceinventory.Repository
}

func NewService(repo Repository, inventoryRepo serviceinventory.Repository) Service {
	return &ServiceMapService{repo: repo, inventoryRepo: inventoryRepo}
}

func (s *ServiceMapService) GetTopology(teamID int64, startMs, endMs int64) (TopologyData, error) {
	ctx := context.Background()

	inventoryServices, err := s.inventoryRepo.ListServices(teamID)
	if err != nil {
		return TopologyData{}, fmt.Errorf("servicemap.ListServices: %w", err)
	}
	inventoryEdges, err := s.inventoryRepo.ListDependencies(teamID)
	if err != nil {
		return TopologyData{}, fmt.Errorf("servicemap.ListDependencies: %w", err)
	}

	nodeRows, err := s.repo.GetTopologyNodes(ctx, teamID, startMs, endMs)
	if err != nil {
		return TopologyData{}, fmt.Errorf("servicemap.GetTopologyNodes: %w", err)
	}
	edgeRows, err := s.repo.GetTopologyEdges(ctx, teamID, startMs, endMs)
	if err != nil {
		return TopologyData{}, fmt.Errorf("servicemap.GetTopologyEdges: %w", err)
	}

	nodes := mergeTopologyNodes(teamID, inventoryServices, nodeRows)
	edges := mergeTopologyEdges(teamID, inventoryEdges, edgeRows)

	return TopologyData{Nodes: nodes, Edges: edges}, nil
}

func (s *ServiceMapService) GetUpstreamDownstream(teamID int64, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error) {
	inventoryEdges, err := s.inventoryRepo.ListDependenciesForService(teamID, serviceName)
	if err != nil {
		return nil, err
	}
	rows, err := s.repo.GetUpstreamDownstream(context.Background(), teamID, serviceName, startMs, endMs)
	if err != nil {
		return nil, err
	}

	metricByKey := make(map[string]serviceDependencyRow, len(rows))
	for _, row := range rows {
		metricByKey[dependencyKey(row.Source, row.Target)] = row
	}

	result := make([]ServiceDependencyDetail, 0, len(inventoryEdges)+len(rows))
	seen := make(map[string]struct{}, len(inventoryEdges))
	for _, edge := range inventoryEdges {
		key := dependencyKey(edge.SourceService, edge.TargetService)
		seen[key] = struct{}{}
		metric, ok := metricByKey[key]
		direction := dependencyDirection(serviceName, edge.SourceService, edge.TargetService)
		result = append(result, ServiceDependencyDetail{
			Source:       edge.SourceService,
			Target:       edge.TargetService,
			CallCount:    metric.CallCount,
			P95LatencyMs: metric.P95LatencyMs,
			ErrorRate:    metric.ErrorRate,
			Direction:    direction,
		})
		if !ok {
			result[len(result)-1].CallCount = 0
		}
	}

	for _, row := range rows {
		key := dependencyKey(row.Source, row.Target)
		if _, ok := seen[key]; ok {
			continue
		}
		result = append(result, ServiceDependencyDetail{
			Source:       row.Source,
			Target:       row.Target,
			CallCount:    row.CallCount,
			P95LatencyMs: row.P95LatencyMs,
			ErrorRate:    row.ErrorRate,
			Direction:    dependencyDirection(serviceName, row.Source, row.Target),
		})
	}

	slices.SortFunc(result, func(a, b ServiceDependencyDetail) int {
		if a.CallCount != b.CallCount {
			if a.CallCount > b.CallCount {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Source+a.Target, b.Source+b.Target)
	})
	return result, nil
}

func (s *ServiceMapService) GetServiceDependencyGraph(teamID int64, serviceName string, startMs, endMs int64) (ServiceDependencyGraph, error) {
	deps, err := s.GetUpstreamDownstream(teamID, serviceName, startMs, endMs)
	if err != nil {
		return ServiceDependencyGraph{}, err
	}

	allInventoryServices, err := s.inventoryRepo.ListServices(teamID)
	if err != nil {
		return ServiceDependencyGraph{}, err
	}
	nodeRows, err := s.repo.GetTopologyNodes(context.Background(), teamID, startMs, endMs)
	if err != nil {
		return ServiceDependencyGraph{}, err
	}

	connectedNames := map[string]struct{}{serviceName: {}}
	edges := make([]TopologyEdge, 0, len(deps))
	for _, dep := range deps {
		connectedNames[dep.Source] = struct{}{}
		connectedNames[dep.Target] = struct{}{}
		edges = append(edges, TopologyEdge{
			Source:            dep.Source,
			Target:            dep.Target,
			CallCount:         dep.CallCount,
			P95LatencyMs:      dep.P95LatencyMs,
			ErrorRate:         dep.ErrorRate,
			WindowHasTraffic:  dep.CallCount > 0,
		})
	}

	inventorySubset := make([]serviceinventory.ServiceRecord, 0, len(connectedNames))
	for _, item := range allInventoryServices {
		if _, ok := connectedNames[item.ServiceName]; ok {
			inventorySubset = append(inventorySubset, item)
		}
	}
	nodes := mergeTopologyNodes(teamID, inventorySubset, nodeRows)

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
	inventoryServices, err := s.inventoryRepo.ListServices(teamID)
	if err != nil {
		return EnrichedTopologyData{}, err
	}
	inventoryByService := make(map[string]serviceinventory.ServiceRecord, len(inventoryServices))
	for _, item := range inventoryServices {
		inventoryByService[item.ServiceName] = item
	}

	enriched := make([]EnrichedTopologyNode, 0, len(topo.Nodes))
	for _, node := range topo.Nodes {
		meta := inventoryByService[node.Name]
		enriched = append(enriched, EnrichedTopologyNode{
			Name:             node.Name,
			Status:           node.Status,
			RequestCount:     node.RequestCount,
			ErrorRate:        node.ErrorRate,
			AvgLatency:       node.AvgLatency,
			ServiceType:      firstNonEmpty(meta.ServiceType, serviceinventory.BuildDefaultServiceObservation(teamID, node.Name, time.Now().UTC(), nil).ServiceType),
			Sparkline:        []int64{},
			ClusterName:      meta.ClusterName,
			LastSeenAt:       firstNonEmpty(node.LastSeenAt, meta.LastSeenAt),
			WindowHasTraffic: node.WindowHasTraffic,
		})
	}

	return EnrichedTopologyData{Nodes: enriched, Edges: topo.Edges}, nil
}

func (s *ServiceMapService) GetTopologyClusters(teamID int64, startMs, endMs int64) ([]TopologyCluster, error) {
	inventoryServices, err := s.inventoryRepo.ListServices(teamID)
	if err != nil {
		return nil, fmt.Errorf("servicemap.GetTopologyClusters: %w", err)
	}

	clusterMap := make(map[string][]string)
	for _, item := range inventoryServices {
		if strings.TrimSpace(item.ClusterName) == "" {
			continue
		}
		clusterMap[item.ClusterName] = append(clusterMap[item.ClusterName], item.ServiceName)
	}

	clusters := make([]TopologyCluster, 0, len(clusterMap))
	for name, services := range clusterMap {
		slices.Sort(services)
		clusters = append(clusters, TopologyCluster{Name: name, Services: services, Count: len(services)})
	}
	slices.SortFunc(clusters, func(a, b TopologyCluster) int { return strings.Compare(a.Name, b.Name) })
	return clusters, nil
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
	for i, row := range rows {
		result[i] = ClientServerLatencyPoint{
			Timestamp:     row.Timestamp,
			OperationName: row.OperationName,
			ClientP95Ms:   row.ClientP95Ms,
			ServerP95Ms:   row.ServerP95Ms,
			NetworkGapMs:  row.ClientP95Ms - row.ServerP95Ms,
		}
	}
	return result, nil
}

func mergeTopologyNodes(teamID int64, inventoryServices []serviceinventory.ServiceRecord, nodeRows []topologyNodeRow) []TopologyNode {
	metricsByService := make(map[string]topologyNodeRow, len(nodeRows))
	for _, row := range nodeRows {
		metricsByService[row.Name] = row
	}

	nodes := make([]TopologyNode, 0, len(inventoryServices)+len(nodeRows))
	seen := make(map[string]struct{}, len(inventoryServices))
	for _, item := range inventoryServices {
		seen[item.ServiceName] = struct{}{}
		if row, ok := metricsByService[item.ServiceName]; ok {
			errorRate := safeErrorRate(row.RequestCount, row.ErrorCount)
			nodes = append(nodes, TopologyNode{
				Name:             item.ServiceName,
				Status:           serviceStatus(errorRate),
				RequestCount:     row.RequestCount,
				ErrorRate:        errorRate,
				AvgLatency:       row.AvgLatency,
				LastSeenAt:       item.LastSeenAt,
				WindowHasTraffic: true,
			})
			continue
		}
		nodes = append(nodes, TopologyNode{
			Name:             item.ServiceName,
			Status:           statusUnknown,
			RequestCount:     0,
			ErrorRate:        0,
			AvgLatency:       0,
			LastSeenAt:       item.LastSeenAt,
			WindowHasTraffic: false,
		})
	}

	for _, row := range nodeRows {
		if _, ok := seen[row.Name]; ok {
			continue
		}
		fallback := serviceinventory.BuildDefaultServiceObservation(teamID, row.Name, time.Now().UTC(), nil)
		errorRate := safeErrorRate(row.RequestCount, row.ErrorCount)
		nodes = append(nodes, TopologyNode{
			Name:             row.Name,
			Status:           serviceStatus(errorRate),
			RequestCount:     row.RequestCount,
			ErrorRate:        errorRate,
			AvgLatency:       row.AvgLatency,
			LastSeenAt:       fallback.LastSeenAt.Format(time.RFC3339),
			WindowHasTraffic: true,
		})
	}

	slices.SortFunc(nodes, compareNodes)
	return nodes
}

func mergeTopologyEdges(teamID int64, inventoryEdges []serviceinventory.DependencyRecord, edgeRows []TopologyEdge) []TopologyEdge {
	metricsByEdge := make(map[string]TopologyEdge, len(edgeRows))
	for _, row := range edgeRows {
		metricsByEdge[dependencyKey(row.Source, row.Target)] = row
	}

	edges := make([]TopologyEdge, 0, len(inventoryEdges)+len(edgeRows))
	seen := make(map[string]struct{}, len(inventoryEdges))
	for _, item := range inventoryEdges {
		key := dependencyKey(item.SourceService, item.TargetService)
		seen[key] = struct{}{}
		if row, ok := metricsByEdge[key]; ok {
			row.LastSeenAt = firstNonEmpty(item.LastSeenAt, row.LastSeenAt)
			row.WindowHasTraffic = row.CallCount > 0
			edges = append(edges, row)
			continue
		}
		edges = append(edges, TopologyEdge{
			Source:            item.SourceService,
			Target:            item.TargetService,
			LastSeenAt:        item.LastSeenAt,
			WindowHasTraffic:  false,
		})
	}

	for _, row := range edgeRows {
		key := dependencyKey(row.Source, row.Target)
		if _, ok := seen[key]; ok {
			continue
		}
		row.WindowHasTraffic = row.CallCount > 0
		if row.LastSeenAt == "" {
			row.LastSeenAt = time.Now().UTC().Format(time.RFC3339)
		}
		edges = append(edges, row)
	}

	slices.SortFunc(edges, compareEdges)
	return edges
}

func dependencyDirection(serviceName, source, target string) string {
	if target == serviceName {
		return "upstream"
	}
	return "downstream"
}

func dependencyKey(source, target string) string {
	return source + "->" + target
}

func safeErrorRate(requestCount, errorCount int64) float64 {
	if requestCount <= 0 {
		return 0
	}
	return float64(errorCount) * 100 / float64(requestCount)
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

func compareNodes(a, b TopologyNode) int {
	if a.WindowHasTraffic != b.WindowHasTraffic {
		if a.WindowHasTraffic {
			return -1
		}
		return 1
	}
	if a.RequestCount != b.RequestCount {
		if a.RequestCount > b.RequestCount {
			return -1
		}
		return 1
	}
	return strings.Compare(a.Name, b.Name)
}

func compareEdges(a, b TopologyEdge) int {
	if a.WindowHasTraffic != b.WindowHasTraffic {
		if a.WindowHasTraffic {
			return -1
		}
		return 1
	}
	if a.CallCount != b.CallCount {
		if a.CallCount > b.CallCount {
			return -1
		}
		return 1
	}
	return strings.Compare(a.Source+a.Target, b.Source+b.Target)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
