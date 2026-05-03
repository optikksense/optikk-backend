package infraconsts

// OpenTelemetry Semantic Conventions for Infrastructure / Resource Utilization Metrics
// Reference: https://opentelemetry.io/docs/specs/semconv/system/

const (
	// Table Names
	TableMetrics = "observability.metrics"

	// Metrics Table Columns
	ColTeamID      = "team_id"
	ColTimestamp   = "timestamp"
	ColMetricName  = "metric_name"
	ColValue       = "value"
	ColServiceName = "service"
	ColHost        = "host"
	ColPod         = "pod"
	ColContainer   = "container"
	ColAttributes  = "attributes"
	ColCount       = "hist_count"

	// CPU Metrics
	MetricSystemCPUUtilization = "system.cpu.utilization"
	MetricSystemCPUUsage       = "system.cpu.usage"
	MetricProcessCPUUsage      = "process.cpu.usage"
	MetricSystemCPUTime        = "system.cpu.time"
	MetricSystemCPULoadAvg1m   = "system.cpu.load_average.1m"
	MetricSystemCPULoadAvg5m   = "system.cpu.load_average.5m"
	MetricSystemCPULoadAvg15m  = "system.cpu.load_average.15m"
	MetricSystemProcessCount   = "system.process.count"

	// Memory Metrics
	MetricSystemMemoryUtilization = "system.memory.utilization"
	MetricSystemMemoryUsage       = "system.memory.usage"
	MetricSystemPagingUsage       = "system.paging.usage"

	// Disk Metrics
	MetricSystemDiskUtilization = "system.disk.utilization"
	MetricSystemDiskIO          = "system.disk.io"
	MetricSystemDiskOperations  = "system.disk.operations"
	MetricSystemDiskIOTime      = "system.disk.io_time"
	MetricSystemFilesystemUsage = "system.filesystem.usage"
	MetricSystemFilesystemUtil  = "system.filesystem.utilization"
	MetricDiskFree              = "disk.free"
	MetricDiskTotal             = "disk.total"

	// Network Metrics
	MetricSystemNetworkUtilization = "system.network.utilization"
	MetricSystemNetworkIO          = "system.network.io"
	MetricSystemNetworkPackets     = "system.network.packets"
	MetricSystemNetworkErrors      = "system.network.errors"
	MetricSystemNetworkDropped     = "system.network.dropped"
	MetricSystemNetworkConnections = "system.network.connections"

	// JVM Metrics
	MetricJVMMemoryUsed        = "jvm.memory.used"
	MetricJVMMemoryMax         = "jvm.memory.max"
	MetricJVMMemoryCommitted   = "jvm.memory.committed"
	MetricJVMMemoryLimit       = "jvm.memory.limit"
	MetricJVMGCDuration        = "jvm.gc.duration"
	MetricJVMThreadCount       = "jvm.thread.count"
	MetricJVMClassLoaded       = "jvm.class.loaded"
	MetricJVMClassCount        = "jvm.class.count"
	MetricJVMCPUTime           = "jvm.cpu.time"
	MetricJVMCPUUtilization    = "jvm.cpu.recent_utilization"
	MetricJVMBufferMemoryUsage = "jvm.buffer.memory.usage"
	MetricJVMBufferCount       = "jvm.buffer.count"

	// Database / Connection Pool Metrics
	MetricDBConnectionPoolUtilization = "db.connection.pool.utilization"
	MetricHikariCPConnectionsActive   = "hikaricp.connections.active"
	MetricHikariCPConnectionsMax      = "hikaricp.connections.max"
	MetricJDBCConnectionsActive       = "jdbc.connections.active"
	MetricJDBCConnectionsMax          = "jdbc.connections.max"

	// Attribute names for dimensional queries
	AttrSystemCPUState              = "system.cpu.state"
	AttrSystemCPUUtilization        = "system.cpu.utilization"
	AttrSystemMemoryState           = "system.memory.state"
	AttrSystemMemoryUtilization     = "system.memory.utilization"
	AttrSystemDiskDirection         = "system.disk.direction"
	AttrSystemDiskUtilization       = "system.disk.utilization"
	AttrSystemNetworkDirection      = "system.network.io.direction"
	AttrSystemNetworkState          = "system.network.state"
	AttrSystemNetworkUtilization    = "system.network.utilization"
	AttrFilesystemMountpoint        = "system.filesystem.mountpoint"
	AttrProcessStatus               = "process.status"
	AttrJVMMemoryType               = "jvm.memory.type"
	AttrJVMMemoryPoolName           = "jvm.memory.pool.name"
	AttrJVMGCName                   = "jvm.gc.name"
	AttrJVMGCAction                 = "jvm.gc.action"
	AttrJVMThreadDaemon             = "jvm.thread.daemon"
	AttrJVMBufferPoolName           = "jvm.buffer.pool.name"
	AttrDBConnectionPoolUtilization = "db.connection_pool.utilization"

	// Percentage Conversion
	PercentageMultiplier = 100.0
	PercentageThreshold  = 1.0
)

// Metric Sets - Grouped metrics for aggregation queries
var (
	CPUMetrics = []string{
		MetricSystemCPUUtilization,
		MetricSystemCPUUsage,
		MetricProcessCPUUsage,
	}

	MemoryMetrics = []string{
		MetricSystemMemoryUtilization,
		MetricJVMMemoryUsed,
		MetricJVMMemoryMax,
	}

	DiskMetrics = []string{
		MetricSystemDiskUtilization,
		MetricDiskFree,
		MetricDiskTotal,
	}

	NetworkMetrics = []string{
		MetricSystemNetworkUtilization,
	}

	ConnectionPoolMetrics = []string{
		MetricDBConnectionPoolUtilization,
		MetricHikariCPConnectionsActive,
		MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive,
		MetricJDBCConnectionsMax,
	}

	AllResourceMetrics = []string{
		MetricSystemCPUUtilization,
		MetricSystemCPUUsage,
		MetricProcessCPUUsage,
		MetricSystemMemoryUtilization,
		MetricJVMMemoryUsed,
		MetricJVMMemoryMax,
		MetricSystemDiskUtilization,
		MetricDiskFree,
		MetricDiskTotal,
		MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization,
		MetricHikariCPConnectionsActive,
		MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive,
		MetricJDBCConnectionsMax,
	}
)

// TimeBucketExpression returns the bucket column expression for infrastructure
// rollup queries. Reads stored ts_bucket directly — no CH-side bucket math;
// see internal/infra/timebucket. startMs/endMs are kept in the signature for
// caller symmetry but no longer affect the grain (storage grain rules).
func TimeBucketExpression(startMs, endMs int64) string {
	_, _ = startMs, endMs
	return "toString(ts_bucket)"
}

func AttrFloat(attrName string) string {
	return "attributes.'" + attrName + "'::Float64"
}
