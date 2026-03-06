package resource_utilisation

// MetricValue represents a single numerical value.
type MetricValue struct {
	Value float64 `json:"value"`
}

// ServiceResource represents resource utilization grouped by service.
type ServiceResource struct {
	ServiceName           string   `json:"service_name"`
	AvgCpuUtil            *float64 `json:"avg_cpu_util"`
	AvgMemoryUtil         *float64 `json:"avg_memory_util"`
	AvgDiskUtil           *float64 `json:"avg_disk_util"`
	AvgNetworkUtil        *float64 `json:"avg_network_util"`
	AvgConnectionPoolUtil *float64 `json:"avg_connection_pool_util"`
	SampleCount           int64    `json:"sample_count"`
}

// InstanceResource represents resource utilization grouped by host, pod, container, and service.
type InstanceResource struct {
	Host                  string   `json:"host"`
	Pod                   string   `json:"pod"`
	Container             string   `json:"container"`
	ServiceName           string   `json:"service_name"`
	AvgCpuUtil            *float64 `json:"avg_cpu_util"`
	AvgMemoryUtil         *float64 `json:"avg_memory_util"`
	AvgDiskUtil           *float64 `json:"avg_disk_util"`
	AvgNetworkUtil        *float64 `json:"avg_network_util"`
	AvgConnectionPoolUtil *float64 `json:"avg_connection_pool_util"`
	SampleCount           int64    `json:"sample_count"`
}

// ResourceBucket represents timeseries data for CPU or Memory utilization.
type ResourceBucket struct {
	Timestamp string   `json:"timestamp"`
	Pod       string   `json:"pod"`
	Value     *float64 `json:"value"`
}

// StateBucket represents timeseries data grouped by a state attribute (e.g. cpu.state, memory.state).
type StateBucket struct {
	Timestamp string   `json:"timestamp"`
	State     string   `json:"state"`
	Value     *float64 `json:"value"`
}

// DirectionBucket represents timeseries data grouped by direction (read/write, transmit/receive).
type DirectionBucket struct {
	Timestamp string   `json:"timestamp"`
	Direction string   `json:"direction"`
	Value     *float64 `json:"value"`
}

// MountpointBucket represents timeseries data grouped by filesystem mountpoint.
type MountpointBucket struct {
	Timestamp  string   `json:"timestamp"`
	Mountpoint string   `json:"mountpoint"`
	Value      *float64 `json:"value"`
}

// LoadAverageResult holds the 1m, 5m, and 15m load average values.
type LoadAverageResult struct {
	Load1m  float64 `json:"load_1m"`
	Load5m  float64 `json:"load_5m"`
	Load15m float64 `json:"load_15m"`
}

// JVMMemoryBucket represents JVM heap/non-heap memory metrics for a pool.
type JVMMemoryBucket struct {
	Timestamp string   `json:"timestamp"`
	PoolName  string   `json:"pool_name"`
	MemType   string   `json:"mem_type"`
	Used      *float64 `json:"used"`
	Committed *float64 `json:"committed"`
	Limit     *float64 `json:"limit"`
}

// JVMClassStats holds JVM class loading statistics.
type JVMClassStats struct {
	Loaded int64 `json:"loaded"`
	Count  int64 `json:"count"`
}

// JVMCPUStats holds JVM CPU time and recent utilization.
type JVMCPUStats struct {
	CPUTimeValue      float64 `json:"cpu_time_value"`
	RecentUtilization float64 `json:"recent_utilization"`
}

// JVMBufferBucket represents JVM buffer pool memory and count metrics.
type JVMBufferBucket struct {
	Timestamp   string   `json:"timestamp"`
	PoolName    string   `json:"pool_name"`
	MemoryUsage *float64 `json:"memory_usage"`
	Count       *float64 `json:"count"`
}

// HistogramSummary holds p50/p95/p99/avg for histogram metrics.
type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Avg float64 `json:"avg"`
}
