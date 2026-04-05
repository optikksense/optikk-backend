package deployments

import "time"

// deploymentAggRow is scanned from ListDeployments aggregation.
type deploymentAggRow struct {
	Version       string    `ch:"version"`
	Environment   string    `ch:"environment"`
	FirstSeen     time.Time `ch:"first_seen"`
	LastSeen      time.Time `ch:"last_seen"`
	SpanCount     int64     `ch:"span_count"`
}

// activeVersionRow is scanned from GetActiveVersion.
type activeVersionRow struct {
	Version       string `ch:"version"`
	Environment   string `ch:"environment"`
}

// impactAggRow is scanned from GetImpactWindow.
type impactAggRow struct {
	RequestCount int64   `ch:"request_count"`
	ErrorCount   int64   `ch:"error_count"`
	P95Ms        float64 `ch:"p95_ms"`
}
