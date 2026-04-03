package servicemap

import "time"

// ServiceDependencyDetail has Direction computed in the service layer (not from DB).
type ServiceDependencyDetail struct {
	Source       string  `json:"source"`
	Target       string  `json:"target"`
	CallCount    int64   `json:"call_count"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	ErrorRate    float64 `json:"error_rate"`
	Direction    string  `json:"direction"` // "upstream" | "downstream" — computed in service
}

type ExternalDependency struct {
	SourceService string  `json:"source_service" ch:"source_service"`
	ExternalHost  string  `json:"external_host"  ch:"external_host"`
	CallCount     int64   `json:"call_count"     ch:"call_count"`
	P95LatencyMs  float64 `json:"p95_latency_ms" ch:"p95_latency_ms"`
	ErrorRate     float64 `json:"error_rate"     ch:"error_rate"`
}

type TopologyNode struct {
	Name             string  `json:"name"`
	Status           string  `json:"status"`
	RequestCount     int64   `json:"request_count"`
	ErrorRate        float64 `json:"error_rate"`
	AvgLatency       float64 `json:"avg_latency"`
	LastSeenAt       string  `json:"last_seen_at"`
	WindowHasTraffic bool    `json:"window_has_traffic"`
}

type TopologyEdge struct {
	Source            string  `json:"source"         ch:"source"`
	Target            string  `json:"target"         ch:"target"`
	CallCount         int64   `json:"call_count"     ch:"call_count"`
	AvgLatency        float64 `json:"avg_latency"    ch:"avg_latency"`
	P95LatencyMs      float64 `json:"p95_latency_ms" ch:"p95_latency_ms"`
	ErrorRate         float64 `json:"error_rate"     ch:"error_rate"`
	LastSeenAt        string  `json:"last_seen_at"`
	WindowHasTraffic  bool    `json:"window_has_traffic"`
}

type TopologyData struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}

// ServiceDependencyGraph is the response for the focused dependency graph endpoint.
type ServiceDependencyGraph struct {
	Center string         `json:"center"`
	Nodes  []TopologyNode `json:"nodes"`
	Edges  []TopologyEdge `json:"edges"`
}

// EnrichedTopologyNode extends TopologyNode with sparkline data and service type.
type EnrichedTopologyNode struct {
	Name             string  `json:"name"`
	Status           string  `json:"status"`
	RequestCount     int64   `json:"request_count"`
	ErrorRate        float64 `json:"error_rate"`
	AvgLatency       float64 `json:"avg_latency"`
	ServiceType      string  `json:"service_type"`
	Sparkline        []int64 `json:"sparkline"`
	ClusterName      string  `json:"cluster_name"`
	LastSeenAt       string  `json:"last_seen_at"`
	WindowHasTraffic bool    `json:"window_has_traffic"`
}

// EnrichedTopologyData is the response for the enriched topology endpoint.
type EnrichedTopologyData struct {
	Nodes []EnrichedTopologyNode `json:"nodes"`
	Edges []TopologyEdge         `json:"edges"`
}

// TopologyCluster represents a group of services.
type TopologyCluster struct {
	Name     string   `json:"name"`
	Services []string `json:"services"`
	Count    int      `json:"count"`
}

// ClientServerLatencyPoint has NetworkGapMs computed in the service layer.
type ClientServerLatencyPoint struct {
	Timestamp     time.Time `json:"timestamp"`
	OperationName string    `json:"operation_name"`
	ClientP95Ms   float64   `json:"client_p95_ms"`
	ServerP95Ms   float64   `json:"server_p95_ms"`
	NetworkGapMs  float64   `json:"network_gap_ms"` // computed in service
}
