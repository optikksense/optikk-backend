package servicemap

import "time"

type ServiceDependencyDetail struct {
	Source       string  `json:"source"`
	Target       string  `json:"target"`
	CallCount    int64   `json:"call_count"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	ErrorRate    float64 `json:"error_rate"`
	Direction    string  `json:"direction"` // "upstream" | "downstream"
}

type ExternalDependency struct {
	SourceService string  `json:"source_service"`
	ExternalHost  string  `json:"external_host"`
	CallCount     int64   `json:"call_count"`
	P95LatencyMs  float64 `json:"p95_latency_ms"`
	ErrorRate     float64 `json:"error_rate"`
}

type ClientServerLatencyPoint struct {
	Timestamp     time.Time `json:"timestamp"`
	OperationName string    `json:"operation_name"`
	ClientP95Ms   float64   `json:"client_p95_ms"`
	ServerP95Ms   float64   `json:"server_p95_ms"`
	NetworkGapMs  float64   `json:"network_gap_ms"`
}
