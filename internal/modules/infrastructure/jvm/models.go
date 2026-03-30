package jvm

type JVMMemoryBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	PoolName  string   `json:"pool_name" ch:"pool_name"`
	MemType   string   `json:"mem_type" ch:"mem_type"`
	Used      *float64 `json:"used" ch:"used"`
	Committed *float64 `json:"committed" ch:"committed"`
	Limit     *float64 `json:"limit" ch:"limit_val"`
}

type HistogramSummary struct {
	P50 float64 `json:"p50" ch:"p50"`
	P95 float64 `json:"p95" ch:"p95"`
	P99 float64 `json:"p99" ch:"p99"`
	Avg float64 `json:"avg" ch:"avg_val"`
}

type JVMClassStats struct {
	Loaded int64 `json:"loaded" ch:"loaded"`
	Count  int64 `json:"count" ch:"count_val"`
}

type JVMCPUStats struct {
	CPUTimeValue      float64 `json:"cpu_time_value" ch:"cpu_time"`
	RecentUtilization float64 `json:"recent_utilization" ch:"cpu_util"`
}

type JVMBufferBucket struct {
	Timestamp   string   `json:"timestamp" ch:"time_bucket"`
	PoolName    string   `json:"pool_name" ch:"pool_name"`
	MemoryUsage *float64 `json:"memory_usage" ch:"memory_usage"`
	Count       *float64 `json:"count" ch:"buf_count"`
}

type JVMGCCollectionBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Collector string   `json:"collector" ch:"collector"`
	Value     *float64 `json:"value" ch:"metric_val"`
}

type JVMThreadBucket struct {
	Timestamp string   `json:"timestamp" ch:"time_bucket"`
	Daemon    string   `json:"daemon" ch:"daemon"`
	Value     *float64 `json:"value" ch:"metric_val"`
}
