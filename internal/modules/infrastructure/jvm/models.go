package jvm

type JVMMemoryBucket struct {
	Timestamp string   `json:"timestamp"`
	PoolName  string   `json:"pool_name"`
	MemType   string   `json:"mem_type"`
	Used      *float64 `json:"used"`
	Committed *float64 `json:"committed"`
	Limit     *float64 `json:"limit"`
}

type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}

type JVMClassStats struct {
	Loaded int64 `json:"loaded"`
	Count  int64 `json:"count"`
}

type JVMCPUStats struct {
	CPUTimeValue      float64 `json:"cpu_time_value"`
	RecentUtilization float64 `json:"recent_utilization"`
}

type JVMBufferBucket struct {
	Timestamp   string   `json:"timestamp"`
	PoolName    string   `json:"pool_name"`
	MemoryUsage *float64 `json:"memory_usage"`
	Count       *float64 `json:"count"`
}

type ResourceBucket struct {
	Timestamp string   `json:"timestamp"`
	Pod       string   `json:"pod"`
	Value     *float64 `json:"value"`
}

type StateBucket struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}
