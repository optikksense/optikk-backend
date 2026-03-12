package topology

type TopologyNode struct {
	Name         string  `json:"name"`
	Status       string  `json:"status"`
	RequestCount int64   `json:"requestCount"`
	ErrorRate    float64 `json:"errorRate"`
	AvgLatency   float64 `json:"avgLatency"`
}

type TopologyEdge struct {
	Source       string  `json:"source"`
	Target       string  `json:"target"`
	CallCount    int64   `json:"callCount"`
	AvgLatency   float64 `json:"avgLatency"`
	P95LatencyMs float64 `json:"p95LatencyMs"`
	ErrorRate    float64 `json:"errorRate"`
}

type TopologyData struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}
