package jvm

import "time"

// ---------------------------------------------------------------------------
// HTTP response DTOs (API contract).
// ---------------------------------------------------------------------------

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

type JVMGCCollectionBucket struct {
	Timestamp string   `json:"timestamp"`
	Collector string   `json:"collector"`
	Value     *float64 `json:"value"`
}

type JVMThreadBucket struct {
	Timestamp string   `json:"timestamp"`
	Daemon    string   `json:"daemon"`
	Value     *float64 `json:"value"`
}

// ---------------------------------------------------------------------------
// Internal repository row types — raw rows out of CH.
// ---------------------------------------------------------------------------

type JVMMemoryRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	PoolName   string    `ch:"pool_name"`
	MemType    string    `ch:"mem_type"`
	MetricName string    `ch:"metric_name"`
	Value      float64   `ch:"value"`
}

type JVMHistogramAggRow struct {
	SumHistSum   float64   `ch:"sum_hist_sum"`
	SumHistCount uint64    `ch:"sum_hist_count"`
	Buckets      []float64 `ch:"hist_buckets"`
	Counts       []uint64  `ch:"hist_counts"`
}

type JVMGCRow struct {
	Timestamp time.Time `ch:"timestamp"`
	GCName    string    `ch:"gc_name"`
	Value     float64   `ch:"value"`
}

type JVMThreadRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Daemon    string    `ch:"daemon"`
	Value     float64   `ch:"value"`
}

type JVMMetricNameRow struct {
	MetricName string  `ch:"metric_name"`
	Value      float64 `ch:"value"`
}

type JVMBufferRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	PoolName   string    `ch:"pool_name"`
	MetricName string    `ch:"metric_name"`
	Value      float64   `ch:"value"`
}
