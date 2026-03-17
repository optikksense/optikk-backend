package topology

type TopologyNode struct {
	Name         string  `json:"name"`
	Status       string  `json:"status"`
	RequestCount int64   `json:"request_count"`
	ErrorRate    float64 `json:"error_rate"`
	AvgLatency   float64 `json:"avg_latency"`
}

type TopologyEdge struct {
	Source       string  `json:"source"        ch:"source"`
	Target       string  `json:"target"        ch:"target"`
	CallCount    int64   `json:"call_count"    ch:"call_count"`
	AvgLatency   float64 `json:"avg_latency"   ch:"avg_latency"`
	P95LatencyMs float64 `json:"p95_latency_ms" ch:"p95_latency_ms"`
	ErrorRate    float64 `json:"error_rate"    ch:"error_rate"`
}

type TopologyData struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}
