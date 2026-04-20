package system

type LatencyTimeSeries struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy    string   `json:"group_by" ch:"group_by"`
	P50Ms      *float64 `json:"p50_ms" ch:"p50_ms"`
	P95Ms      *float64 `json:"p95_ms" ch:"p95_ms"`
	P99Ms      *float64 `json:"p99_ms" ch:"p99_ms"`

	LatencySum   float64 `json:"-" ch:"-"`
	LatencyCount int64   `json:"-" ch:"-"`
}

type OpsTimeSeries struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy    string   `json:"group_by" ch:"group_by"`
	OpsPerSec  *float64 `json:"ops_per_sec" ch:"ops_per_sec"`
}

type ErrorTimeSeries struct {
	TimeBucket   string   `json:"time_bucket" ch:"time_bucket"`
	GroupBy      string   `json:"group_by" ch:"group_by"`
	ErrorsPerSec *float64 `json:"errors_per_sec" ch:"errors_per_sec"`
}

type SystemCollectionRow struct {
	CollectionName string   `json:"collection_name" ch:"collection_name"`
	P99Ms          *float64 `json:"p99_ms" ch:"p99_ms"`
	OpsPerSec      *float64 `json:"ops_per_sec" ch:"ops_per_sec"`

	LatencySum   float64 `json:"-" ch:"-"`
	LatencyCount int64   `json:"-" ch:"-"`
}

type SystemNamespace struct {
	Namespace string `json:"namespace" ch:"namespace"`
	SpanCount int64  `json:"span_count" ch:"span_count"`
}
