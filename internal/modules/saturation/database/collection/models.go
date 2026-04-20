package collection

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

type CollectionTopQuery struct {
	QueryText  string   `json:"query_text" ch:"query_text"`
	P99Ms      *float64 `json:"p99_ms" ch:"p99_ms"`
	CallCount  int64    `json:"call_count" ch:"call_count"`
	ErrorCount int64    `json:"error_count" ch:"error_count"`

	DBSystem             string `json:"-" ch:"db_system"`
	QueryTextFingerprint string `json:"-" ch:"query_fingerprint"`
}

type ReadWritePoint struct {
	TimeBucket     string   `json:"time_bucket" ch:"time_bucket"`
	ReadOpsPerSec  *float64 `json:"read_ops_per_sec" ch:"read_ops_per_sec"`
	WriteOpsPerSec *float64 `json:"write_ops_per_sec" ch:"write_ops_per_sec"`
}
