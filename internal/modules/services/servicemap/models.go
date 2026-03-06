package servicemap

import "time"

// ServiceDependencyDetail is a dependency edge with latency and error metrics.
type ServiceDependencyDetail struct {
	Source      string  `json:"source"`
	Target      string  `json:"target"`
	CallCount   int64   `json:"callCount"`
	P95LatencyMs float64 `json:"p95LatencyMs"`
	ErrorRate   float64 `json:"errorRate"`
	Direction   string  `json:"direction"` // "upstream" | "downstream"
}

// ExternalDependency represents a call to a host outside the known service mesh.
type ExternalDependency struct {
	SourceService string  `json:"sourceService"`
	ExternalHost  string  `json:"externalHost"`
	CallCount     int64   `json:"callCount"`
	P95LatencyMs  float64 `json:"p95LatencyMs"`
	ErrorRate     float64 `json:"errorRate"`
}

// ClientServerLatencyPoint is a time-series point comparing client vs server duration.
type ClientServerLatencyPoint struct {
	Timestamp      time.Time `json:"timestamp"`
	OperationName  string    `json:"operationName"`
	ClientP95Ms    float64   `json:"clientP95Ms"`
	ServerP95Ms    float64   `json:"serverP95Ms"`
	NetworkGapMs   float64   `json:"networkGapMs"`
}
