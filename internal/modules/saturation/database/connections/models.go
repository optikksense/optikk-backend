package connections

type ConnectionCountPoint struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	PoolName   string   `json:"pool_name" ch:"pool_name"`
	State      string   `json:"state" ch:"state"`
	Count      *float64 `json:"count" ch:"count"`
}

type ConnectionUtilPoint struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	PoolName   string   `json:"pool_name" ch:"pool_name"`
	UtilPct    *float64 `json:"util_pct" ch:"util_pct"`
}

type ConnectionLimits struct {
	PoolName string   `json:"pool_name" ch:"pool_name"`
	Max      *float64 `json:"max" ch:"max_val"`
	IdleMax  *float64 `json:"idle_max" ch:"idle_max"`
	IdleMin  *float64 `json:"idle_min" ch:"idle_min"`
}

type PendingRequestsPoint struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	PoolName   string   `json:"pool_name" ch:"pool_name"`
	Count      *float64 `json:"count" ch:"count"`
}

type ConnectionTimeoutPoint struct {
	TimeBucket  string   `json:"time_bucket" ch:"time_bucket"`
	PoolName    string   `json:"pool_name" ch:"pool_name"`
	TimeoutRate *float64 `json:"timeout_rate" ch:"timeout_rate"`
}

type PoolLatencyPoint struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	PoolName   string   `json:"pool_name" ch:"pool_name"`
	P50Ms      *float64 `json:"p50_ms" ch:"p50_ms"`
	P95Ms      *float64 `json:"p95_ms" ch:"p95_ms"`
	P99Ms      *float64 `json:"p99_ms" ch:"p99_ms"`
}
