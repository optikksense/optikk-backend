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

// ServiceLatestDeployment is the most recent observed deployment for a service.
type ServiceLatestDeployment struct {
	ServiceName string    `json:"service_name"`
	Version     string    `json:"version"`
	Environment string    `json:"environment"`
	DeployedAt  time.Time `json:"deployed_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
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
	P99Ms        float64 `json:"p99_ms"`
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

type DeploymentCompareWindow struct {
	StartMs int64 `json:"start_ms"`
	EndMs   int64 `json:"end_ms"`
}

type DeploymentCompareSummary struct {
	Before *ImpactWindowMetrics `json:"before,omitempty"`
	After  ImpactWindowMetrics  `json:"after"`
}

type DeploymentCompareErrorRegression struct {
	GroupID        string    `json:"group_id"`
	OperationName  string    `json:"operation_name"`
	StatusMessage  string    `json:"status_message"`
	HTTPStatusCode int       `json:"http_status_code"`
	BeforeCount    uint64    `json:"before_count"`
	AfterCount     uint64    `json:"after_count"`
	DeltaCount     int64     `json:"delta_count"`
	LastOccurrence time.Time `json:"last_occurrence"`
	SampleTraceID  string    `json:"sample_trace_id"`
	Severity       string    `json:"severity"`
}

type DeploymentCompareEndpointRegression struct {
	EndpointName    string  `json:"endpoint_name"`
	OperationName   string  `json:"operation_name"`
	HTTPMethod      string  `json:"http_method"`
	BeforeRequests  int64   `json:"before_requests"`
	AfterRequests   int64   `json:"after_requests"`
	RequestDelta    int64   `json:"request_delta"`
	BeforeErrorRate float64 `json:"before_error_rate"`
	AfterErrorRate  float64 `json:"after_error_rate"`
	ErrorRateDelta  float64 `json:"error_rate_delta"`
	BeforeP95Ms     float64 `json:"before_p95_ms"`
	AfterP95Ms      float64 `json:"after_p95_ms"`
	P95DeltaMs      float64 `json:"p95_delta_ms"`
	BeforeP99Ms     float64 `json:"before_p99_ms"`
	AfterP99Ms      float64 `json:"after_p99_ms"`
	P99DeltaMs      float64 `json:"p99_delta_ms"`
	RegressionScore float64 `json:"regression_score"`
}

type DeploymentCompareResponse struct {
	Deployment      ServiceLatestDeployment               `json:"deployment"`
	BeforeWindow    *DeploymentCompareWindow              `json:"before_window,omitempty"`
	AfterWindow     DeploymentCompareWindow               `json:"after_window"`
	HasBaseline     bool                                  `json:"has_baseline"`
	Summary         DeploymentCompareSummary              `json:"summary"`
	TopErrors       []DeploymentCompareErrorRegression    `json:"top_errors"`
	TopEndpoints    []DeploymentCompareEndpointRegression `json:"top_endpoints"`
	TimelineStartMs int64                                 `json:"timeline_start_ms"`
	TimelineEndMs   int64                                 `json:"timeline_end_ms"`
}
