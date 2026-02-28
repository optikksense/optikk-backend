package model

// TopologyNode represents a node in the service topology graph.
type TopologyNode struct {
	Name         string  `json:"name"`
	Status       string  `json:"status"`
	RequestCount int64   `json:"requestCount"`
	ErrorRate    float64 `json:"errorRate"`
	AvgLatency   float64 `json:"avgLatency"`
}

// TopologyEdge represents an edge in the service topology graph.
type TopologyEdge struct {
	Source     string  `json:"source"`
	Target     string  `json:"target"`
	CallCount  int64   `json:"callCount"`
	AvgLatency float64 `json:"avgLatency"`
	ErrorRate  float64 `json:"errorRate"`
}

// TopologyData represents the complete topology graph.
type TopologyData struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}
