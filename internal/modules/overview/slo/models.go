package slo

type Response struct {
	Objectives Objectives  `json:"objectives"`
	Status     Status      `json:"status"`
	Summary    Summary     `json:"summary"`
	Timeseries []TimeSlice `json:"timeseries"`
}

type Objectives struct {
	AvailabilityTarget float64 `json:"availability_target"`
	P95LatencyTargetMs float64 `json:"p95_latency_target_ms"`
}

type Status struct {
	AvailabilityPercent         float64 `json:"availability_percent"`
	P95LatencyMs                float64 `json:"p95_latency_ms"`
	ErrorBudgetRemainingPercent float64 `json:"error_budget_remaining_percent"`
	Compliant                   bool    `json:"compliant"`
}

type Summary struct {
	TotalRequests       int64   `json:"total_requests"`
	ErrorCount          int64   `json:"error_count"`
	AvailabilityPercent float64 `json:"availability_percent"`
	AvgLatencyMs        float64 `json:"avg_latency_ms"`
	P95LatencyMs        float64 `json:"p95_latency_ms"`
}

type TimeSlice struct {
	Timestamp           string   `json:"timestamp"`
	RequestCount        int64    `json:"request_count"`
	ErrorCount          int64    `json:"error_count"`
	AvailabilityPercent float64  `json:"availability_percent"`
	AvgLatencyMs        *float64 `json:"avg_latency_ms"`
}
