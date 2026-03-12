package apm

type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}

type StateBucket struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}

type TimeBucket struct {
	Timestamp string   `json:"timestamp"`
	Value     *float64 `json:"value"`
}

type ProcessMemory struct {
	RSS float64 `json:"rss"`
	VMS float64 `json:"vms"`
}
