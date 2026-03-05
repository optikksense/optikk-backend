package resource_utilisation

import (
	"strings"

	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// OpenTelemetry Semantic Conventions for Resource Utilization Metrics
// Based on OpenTelemetry Semantic Conventions for System Metrics
// Reference: https://opentelemetry.io/docs/specs/semconv/system/

const (
	// Table Names
	TableMetrics = "metrics_v5"

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
	ColAvg         = "value"
	ColMax         = "value"
	ColMin         = "value"

	// Standard OpenTelemetry System Metrics
	MetricSystemCPUUtilization     = "system.cpu.utilization"
	MetricSystemCPUUsage           = "system.cpu.usage"
	MetricProcessCPUUsage          = "process.cpu.usage"
	MetricSystemMemoryUtilization  = "system.memory.utilization"
	MetricSystemDiskUtilization    = "system.disk.utilization"
	MetricSystemNetworkUtilization = "system.network.utilization"

	// JVM Metrics (OpenTelemetry JVM conventions)
	MetricJVMMemoryUsed = "jvm.memory.used"
	MetricJVMMemoryMax  = "jvm.memory.max"

	// Database Connection Pool Metrics (OpenTelemetry DB conventions)
	MetricDBConnectionPoolUtilization = "db.connection.pool.utilization"

	// HikariCP Metrics (vendor-specific)
	MetricHikariCPConnectionsActive = "hikaricp.connections.active"
	MetricHikariCPConnectionsMax    = "hikaricp.connections.max"

	// JDBC Metrics (vendor-specific)
	MetricJDBCConnectionsActive = "jdbc.connections.active"
	MetricJDBCConnectionsMax    = "jdbc.connections.max"

	// Disk Metrics (vendor-specific)
	MetricDiskFree  = "disk.free"
	MetricDiskTotal = "disk.total"

	// Resource Attributes - OpenTelemetry Semantic Conventions
	AttrSystemCPUUtilization        = "system.cpu.utilization"
	AttrSystemMemoryUtilization     = "system.memory.utilization"
	AttrSystemDiskUtilization       = "system.disk.utilization"
	AttrSystemNetworkUtilization    = "system.network.utilization"
	AttrDBConnectionPoolUtilization = "db.connection_pool.utilization"

	// Percentage Conversion
	PercentageMultiplier = 100.0
	PercentageThreshold  = 1.0

	// Time Bucketing Intervals (in milliseconds)
	ThreeHours      = 3 * 3_600_000
	TwentyFourHours = 24 * 3_600_000
	OneWeek         = 168 * 3_600_000

	// Time Format
	TimeFormatISO8601 = "%Y-%m-%dT%H:%i:%SZ"
)

// Metric Sets - Grouped metrics for easier querying
var (
	// CPUMetrics - All CPU-related metrics
	CPUMetrics = []string{
		MetricSystemCPUUtilization,
		MetricSystemCPUUsage,
		MetricProcessCPUUsage,
	}

	// MemoryMetrics - All memory-related metrics
	MemoryMetrics = []string{
		MetricSystemMemoryUtilization,
		MetricJVMMemoryUsed,
		MetricJVMMemoryMax,
	}

	// DiskMetrics - All disk-related metrics
	DiskMetrics = []string{
		MetricSystemDiskUtilization,
		MetricDiskFree,
		MetricDiskTotal,
	}

	// NetworkMetrics - All network-related metrics
	NetworkMetrics = []string{
		MetricSystemNetworkUtilization,
	}

	// ConnectionPoolMetrics - All connection pool-related metrics
	ConnectionPoolMetrics = []string{
		MetricDBConnectionPoolUtilization,
		MetricHikariCPConnectionsActive,
		MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive,
		MetricJDBCConnectionsMax,
	}

	// AllResourceMetrics - All resource utilization metrics
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

// TimeBucketExpression returns the appropriate time bucket expression based on time range.
// Delegates to the shared timebucket package.
func TimeBucketExpression(startMs, endMs int64) string {
	return timebucket.Expression(startMs, endMs)
}

// MetricSetToInClause converts a metric set to a SQL IN clause
// Example: MetricSetToInClause(CPUMetrics) returns "'metric1', 'metric2', 'metric3'"
func MetricSetToInClause(metrics []string) string {
	if len(metrics) == 0 {
		return ""
	}
	var builder strings.Builder
	for i, metric := range metrics {
		if i > 0 {
			builder.WriteString("', '")
		} else {
			builder.WriteString("'")
		}
		builder.WriteString(metric)
	}
	builder.WriteString("'")
	return builder.String()
}
