package connections

type ConnectionCountPoint struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	PoolName   string   `json:"pool_name" ch:"pool_name"`
	State      string   `json:"state" ch:"state"`
	Count      *float64 `json:"count"`

	CountSum float64 `json:"-" ch:"value_sum"`
	CountN   uint64  `json:"-" ch:"value_count"`
}

// ConnectionUtilPoint carries raw used/max sum+count per (time_bucket,
// pool_name). Service computes util_pct in Go as:
//
//	used_avg = UsedSum / UsedN   ; max_avg = MaxSum / MaxN
//	util_pct = used_avg / max_avg * 100
type ConnectionUtilPoint struct {
	TimeBucket string   `json:"time_bucket"`
	PoolName   string   `json:"pool_name"`
	UtilPct    *float64 `json:"util_pct"`

	UsedSum float64 `json:"-"`
	UsedN   int64   `json:"-"`
	MaxSum  float64 `json:"-"`
	MaxN    int64   `json:"-"`
}

type ConnectionLimits struct {
	PoolName string   `json:"pool_name"`
	Max      *float64 `json:"max"`
	IdleMax  *float64 `json:"idle_max"`
	IdleMin  *float64 `json:"idle_min"`

	MaxSum     float64 `json:"-"`
	MaxN       int64   `json:"-"`
	IdleMaxSum float64 `json:"-"`
	IdleMaxN   int64   `json:"-"`
	IdleMinSum float64 `json:"-"`
	IdleMinN   int64   `json:"-"`
}

type PendingRequestsPoint struct {
	TimeBucket string   `json:"time_bucket" ch:"time_bucket"`
	PoolName   string   `json:"pool_name" ch:"pool_name"`
	Count      *float64 `json:"count"`

	CountSum float64 `json:"-" ch:"value_sum"`
	CountN   uint64  `json:"-" ch:"value_count"`
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

	LatencySum   float64 `json:"-" ch:"latency_sum"`
	LatencyCount int64   `json:"-" ch:"latency_count"`
}
