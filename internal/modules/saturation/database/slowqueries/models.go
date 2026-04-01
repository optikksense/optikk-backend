package slowqueries

type SlowQueryPattern struct {
	QueryText      string   `json:"query_text" ch:"query_text"`
	CollectionName string   `json:"collection_name" ch:"collection_name"`
	P50Ms          *float64 `json:"p50_ms" ch:"p50_ms"`
	P95Ms          *float64 `json:"p95_ms" ch:"p95_ms"`
	P99Ms          *float64 `json:"p99_ms" ch:"p99_ms"`
	CallCount      int64    `json:"call_count" ch:"call_count"`
	ErrorCount     int64    `json:"error_count" ch:"error_count"`
}

type SlowCollectionRow struct {
	CollectionName string   `json:"collection_name" ch:"collection_name"`
	P99Ms          *float64 `json:"p99_ms" ch:"p99_ms"`
	OpsPerSec      *float64 `json:"ops_per_sec" ch:"ops_per_sec"`
	ErrorRate      *float64 `json:"error_rate" ch:"error_rate"`
}

type SlowRatePoint struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	SlowPerSec *float64 `json:"slow_per_sec" ch:"slow_per_sec"`
}

type P99ByQueryText struct {
	QueryText string   `json:"query_text" ch:"query_text"`
	P99Ms     *float64 `json:"p99_ms" ch:"p99_ms"`
}
