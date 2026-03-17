package apm

type HistogramSummary struct {
	P50 float64 `json:"p50" ch:"p50"`
	P95 float64 `json:"p95" ch:"p95"`
	P99 float64 `json:"p99" ch:"p99"`
	Avg float64 `json:"avg" ch:"avg"`
}

type StateBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	State     string   `json:"state"     ch:"state"`
	Value     *float64 `json:"value"     ch:"val"`
}

type TimeBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Value     *float64 `json:"value"     ch:"val"`
}

type ProcessMemory struct {
	RSS float64 `json:"rss" ch:"rss"`
	VMS float64 `json:"vms" ch:"vms"`
}
