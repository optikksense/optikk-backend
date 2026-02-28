package model

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
