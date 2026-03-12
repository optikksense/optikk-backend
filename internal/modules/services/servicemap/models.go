package servicemap

import "time"

type ServiceDependencyDetail struct {
	Source      string  `json:"source"`
	Target      string  `json:"target"`
	CallCount   int64   `json:"callCount"`
	P95LatencyMs float64 `json:"p95LatencyMs"`
	ErrorRate   float64 `json:"errorRate"`
	Direction   string  `json:"direction"` // "upstream" | "downstream"
}

type ExternalDependency struct {
	SourceService string  `json:"sourceService"`
	ExternalHost  string  `json:"externalHost"`
	CallCount     int64   `json:"callCount"`
	P95LatencyMs  float64 `json:"p95LatencyMs"`
	ErrorRate     float64 `json:"errorRate"`
}

type ClientServerLatencyPoint struct {
	Timestamp      time.Time `json:"timestamp"`
	OperationName  string    `json:"operationName"`
	ClientP95Ms    float64   `json:"clientP95Ms"`
	ServerP95Ms    float64   `json:"serverP95Ms"`
	NetworkGapMs   float64   `json:"networkGapMs"`
}
