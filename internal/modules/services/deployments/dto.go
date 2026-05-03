package deployments

import "time"

// deploymentAggRow is scanned from ListDeployments aggregation.
type deploymentAggRow struct {
	ServiceName  string    `ch:"service"`
	Version      string    `ch:"version"`
	Environment  string    `ch:"environment"`
	FirstSeen    time.Time `ch:"first_seen"`
	LastSeen     time.Time `ch:"last_seen"`
	SpanCount    int64     `ch:"span_count"`
	CommitSHA    string    `ch:"commit_sha"`
	CommitAuthor string    `ch:"commit_author"`
	RepoURL      string    `ch:"repo_url"`
	PRURL        string    `ch:"pr_url"`
}

// activeVersionRow is scanned from GetActiveVersion.
type activeVersionRow struct {
	Version     string `ch:"version"`
	Environment string `ch:"environment"`
}

// impactAggRow is scanned from GetImpactWindow.
type impactAggRow struct {
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	P95Ms        float64 `ch:"p95_ms"`
	P99Ms        float64 `ch:"p99_ms"`
}

type errorGroupAggRow struct {
	ServiceName    string    `ch:"service"`
	GroupID        string    `ch:"group_id"`
	OperationName  string    `ch:"operation_name"`
	StatusMessage  string    `ch:"status_message"`
	HTTPStatusCode int       `ch:"http_status_code"`
	ErrorCount     uint64    `ch:"error_count"`
	LastOccurrence time.Time `ch:"last_occurrence"`
	SampleTraceID  string    `ch:"sample_trace_id"`
}

type endpointMetricAggRow struct {
	OperationName string  `ch:"operation_name"`
	EndpointName  string  `ch:"endpoint_name"`
	HTTPMethod    string  `ch:"http_method"`
	RequestCount  int64   `ch:"request_count"`
	ErrorCount    int64   `ch:"error_count"`
	P95Ms         float64 `ch:"p95_ms"`
	P99Ms         float64 `ch:"p99_ms"`
}
