package workloads

import "time"

type Workload struct {
	ID           int64     `json:"id"`
	TeamID       int64     `json:"teamId"`
	CreatedBy    int64     `json:"createdBy"`
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	ServiceNames []string  `json:"serviceNames"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

type WorkloadHealth struct {
	Workload
	HealthStatus string  `json:"healthStatus"` // healthy, degraded, critical
	ServiceCount int     `json:"serviceCount"`
	ErrorRate    float64 `json:"errorRate"`
	P95LatencyMs float64 `json:"p95LatencyMs"`
}

type CreateWorkloadRequest struct {
	Name         string   `json:"name" binding:"required"`
	Description  string   `json:"description"`
	ServiceNames []string `json:"serviceNames" binding:"required"`
}

type UpdateWorkloadRequest struct {
	Name         *string   `json:"name"`
	Description  *string   `json:"description"`
	ServiceNames *[]string `json:"serviceNames"`
}
