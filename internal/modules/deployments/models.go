package deployments

import "time"

// Deployment is a distinct (version, environment) observed for a service in the time range.
type Deployment struct {
	Version     string    `json:"version"`
	Environment string    `json:"environment"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
	SpanCount   int64     `json:"span_count"`
	IsActive    bool      `json:"is_active"`
}

// ListDeploymentsResponse is the payload for GET /deployments/list.
// Top-level scalar fields support stat-card valueField without nested paths.
type ListDeploymentsResponse struct {
	Deployments       []Deployment `json:"deployments"`
	Total             int          `json:"total"`
	ActiveVersion     string       `json:"active_version"`
	ActiveEnvironment string       `json:"active_environment,omitempty"`
}

// VersionTrafficPoint is one time bucket × version for the request-rate chart.
type VersionTrafficPoint struct {
	Timestamp time.Time `json:"timestamp" ch:"timestamp"`
	Version   string    `json:"version"   ch:"version"`
	RPS       float64   `json:"rps"       ch:"rps"`
}

// ImpactWindowMetrics is aggregated metrics for a single time window.
type ImpactWindowMetrics struct {
	RequestCount int64   `json:"request_count"`
	ErrorCount   int64   `json:"error_count"`
	ErrorRate    float64 `json:"error_rate"`
	P95Ms        float64 `json:"p95_ms"`
	RPS          float64 `json:"rps"`
}

// DeploymentImpactRow is one row in the impact table (MVP).
type DeploymentImpactRow struct {
	Version     string `json:"version"`
	Environment string `json:"environment"`

	ErrorRateBefore *float64 `json:"error_rate_before,omitempty"`
	ErrorRateAfter  *float64 `json:"error_rate_after,omitempty"`
	ErrorRateDelta  *float64 `json:"error_rate_delta,omitempty"`

	P95Before *float64 `json:"p95_before,omitempty"`
	P95After  *float64 `json:"p95_after,omitempty"`
	P95Delta  *float64 `json:"p95_delta,omitempty"`

	RPSBefore *float64 `json:"rps_before,omitempty"`
	RPSAfter  *float64 `json:"rps_after,omitempty"`
	RPSDelta  *float64 `json:"rps_delta,omitempty"`

	IsBaseline bool `json:"is_baseline"`
}

// DeploymentImpactResponse wraps impact rows.
type DeploymentImpactResponse struct {
	Impacts []DeploymentImpactRow `json:"impacts"`
}

// ActiveVersionResponse is returned by GET /deployments/active-version.
type ActiveVersionResponse struct {
	Version     string `json:"version"`
	Environment string `json:"environment"`
}
