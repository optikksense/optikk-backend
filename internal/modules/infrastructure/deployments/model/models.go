package model

import "time"

// Deployment represents a deployment event in the system.
type Deployment struct {
	DeployID        string         `json:"deploy_id"`
	ServiceName     string         `json:"service_name"`
	Version         string         `json:"version"`
	Environment     string         `json:"environment"`
	DeployedBy      string         `json:"deployed_by"`
	DeployTime      time.Time      `json:"deploy_time"`
	Status          string         `json:"status"`
	CommitSHA       string         `json:"commit_sha"`
	DurationSeconds int            `json:"duration_seconds"`
	Attributes      map[string]any `json:"attributes"`
}

// DeploymentEvent is a lightweight representation for timeline overlays.
type DeploymentEvent struct {
	DeployID    string    `json:"deploy_id"`
	ServiceName string    `json:"service_name"`
	Version     string    `json:"version"`
	DeployTime  time.Time `json:"deploy_time"`
	Status      string    `json:"status"`
	Environment string    `json:"environment"`
}

// DeploymentDiff represents performance differences before and after a deployment.
type DeploymentDiff struct {
	DeployID           string    `json:"deploy_id"`
	DeployTime         time.Time `json:"deploy_time"`
	ServiceName        string    `json:"service_name"`
	WindowMinutes      int       `json:"window_minutes"`
	AvgLatencyBefore   float64   `json:"avg_latency_before"`
	P95LatencyBefore   float64   `json:"p95_latency_before"`
	ErrorRateBefore    float64   `json:"error_rate_before"`
	RequestCountBefore int64     `json:"request_count_before"`
	AvgLatencyAfter    float64   `json:"avg_latency_after"`
	P95LatencyAfter    float64   `json:"p95_latency_after"`
	ErrorRateAfter     float64   `json:"error_rate_after"`
	RequestCountAfter  int64     `json:"request_count_after"`
}
