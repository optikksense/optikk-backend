package resource_utilisation

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// resBucketExpr returns a ClickHouse time-bucketing expression for adaptive granularity.
// This is a wrapper around TimeBucketExpression from otel_conventions.go
func resBucketExpr(startMs, endMs int64) string {
	return TimeBucketExpression(startMs, endMs)
}

// Repository encapsulates data access logic for resource utilization.
type Repository interface {
	GetAvgCPU(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(teamID int64, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(teamID int64, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(teamID int64, startMs, endMs int64) ([]InstanceResource, error)

	// System infrastructure metrics
	GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error)
	GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetNetworkIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkPackets(teamID int64, startMs, endMs int64) ([]DirectionBucket, error)
	GetNetworkErrors(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetNetworkDropped(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error)
	GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetNetworkConnections(teamID int64, startMs, endMs int64) ([]StateBucket, error)

	// JVM runtime metrics
	GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error)
	GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error)
	GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error)
	GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error)
	GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error)
	GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new Resource Utilization Repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

// queryDiskMetricByService queries disk utilization metrics for a service
func (r *ClickHouseRepository) queryDiskMetricByService(teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aDisk := attrFloat(AttrSystemDiskUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_disk,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * (1.0 - (sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0))),
			   NULL) as ratio_disk,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_disk
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemDiskUtilization, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDiskFree, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		aDisk, fmt.Sprintf("%.1f", PercentageThreshold), aDisk, fmt.Sprintf("%.1f", PercentageMultiplier), aDisk, aDisk,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal,
		aDisk)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_disk"]),
		dbutil.Float64FromAny(row["ratio_disk"]),
		dbutil.Float64FromAny(row["attr_disk"]),
	}
	return calculateAverage(values), nil
}

// queryNetworkMetricByService queries network utilization metrics for a service
func (r *ClickHouseRepository) queryNetworkMetricByService(teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aNet := attrFloat(AttrSystemNetworkUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_net,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_net
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s = '%s'
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemNetworkUtilization, ColValue,
		aNet, fmt.Sprintf("%.1f", PercentageThreshold), aNet, fmt.Sprintf("%.1f", PercentageMultiplier), aNet, aNet,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemNetworkUtilization,
		aNet)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_net"]),
		dbutil.Float64FromAny(row["attr_net"]),
	}
	return calculateAverage(values), nil
}

// queryConnectionPoolMetricByService queries connection pool utilization metrics for a service
func (r *ClickHouseRepository) queryConnectionPoolMetricByService(teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as hikari_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jdbc_conn,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_conn
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDBConnectionPoolUtilization, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricHikariCPConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJDBCConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		aConn, fmt.Sprintf("%.1f", PercentageThreshold), aConn, fmt.Sprintf("%.1f", PercentageMultiplier), aConn, aConn,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax, MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		aConn)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_conn"]),
		dbutil.Float64FromAny(row["hikari_conn"]),
		dbutil.Float64FromAny(row["jdbc_conn"]),
		dbutil.Float64FromAny(row["attr_conn"]),
	}
	return calculateAverage(values), nil
}

// Helper function to calculate average from non-null values
func calculateAverage(values []float64) *float64 {
	var sum float64
	count := 0
	for _, v := range values {
		if v >= 0 { // Only count valid values
			sum += v
			count++
		}
	}
	if count == 0 {
		return nil
	}
	avg := sum / float64(count)
	return &avg
}

// syncAverageExpr creates a ClickHouse expression that averages non-null values
// This is still used by getByInstanceQuerySelectFull for complex aggregations
func syncAverageExpr(parts ...string) string {
	joined := strings.Join(parts, ", ")
	return `if(
		length(arrayFilter(x -> isNotNull(x), [` + joined + `])) > 0,
		arrayReduce('avg', arrayFilter(x -> isNotNull(x), [` + joined + `])),
		NULL
	)`
}

// queryCPUMetricByService queries CPU utilization metrics for a service
func (r *ClickHouseRepository) queryCPUMetricByService(teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) as system_cpu,
			avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) as process_cpu,
			avgIf(%s * %s, %s >= 0 AND %s <= %s) as attr_cpu
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		aCPU, fmt.Sprintf("%.1f", PercentageMultiplier), aCPU, aCPU, fmt.Sprintf("%.1f", PercentageThreshold),
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage,
		aCPU)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_cpu"]),
		dbutil.Float64FromAny(row["process_cpu"]),
		dbutil.Float64FromAny(row["attr_cpu"]),
	}
	return calculateAverage(values), nil
}

// queryMemoryMetricByService queries memory utilization metrics for a service
func (r *ClickHouseRepository) queryMemoryMetricByService(teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aMem := attrFloat(AttrSystemMemoryUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_mem,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jvm_mem,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_mem
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemMemoryUtilization, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJVMMemoryUsed, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		aMem, fmt.Sprintf("%.1f", PercentageThreshold), aMem, fmt.Sprintf("%.1f", PercentageMultiplier), aMem, aMem,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		aMem)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_mem"]),
		dbutil.Float64FromAny(row["jvm_mem"]),
		dbutil.Float64FromAny(row["attr_mem"]),
	}
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) GetAvgCPU(teamID int64, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query CPU metric for each service and calculate average
	var values []float64
	for _, service := range services {
		cpuVal, err := r.queryCPUMetricByService(teamID, service, startMs, endMs)
		if err == nil && cpuVal != nil && *cpuVal >= 0 {
			values = append(values, *cpuVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetAvgMemory(teamID int64, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query memory metric for each service and calculate average
	var values []float64
	for _, service := range services {
		memVal, err := r.queryMemoryMetricByService(teamID, service, startMs, endMs)
		if err == nil && memVal != nil && *memVal >= 0 {
			values = append(values, *memVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetAvgNetwork(teamID int64, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query network metric for each service and calculate average
	var values []float64
	for _, service := range services {
		netVal, err := r.queryNetworkMetricByService(teamID, service, startMs, endMs)
		if err == nil && netVal != nil && *netVal >= 0 {
			values = append(values, *netVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetAvgConnPool(teamID int64, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query connection pool metric for each service and calculate average
	var values []float64
	for _, service := range services {
		connVal, err := r.queryConnectionPoolMetricByService(teamID, service, startMs, endMs)
		if err == nil && connVal != nil && *connVal >= 0 {
			values = append(values, *connVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

// queryCPUMetricByInstance queries CPU utilization metrics for a specific instance
func (r *ClickHouseRepository) queryCPUMetricByInstance(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) as system_cpu,
			avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) as process_cpu,
			avgIf(%s * %s, %s >= 0 AND %s <= %s) as attr_cpu
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		aCPU, fmt.Sprintf("%.1f", PercentageMultiplier), aCPU, aCPU, fmt.Sprintf("%.1f", PercentageThreshold),
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage,
		aCPU)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_cpu"]),
		dbutil.Float64FromAny(row["process_cpu"]),
		dbutil.Float64FromAny(row["attr_cpu"]),
	}
	return calculateAverage(values), nil
}

// queryMemoryMetricByInstance queries memory utilization metrics for a specific instance
func (r *ClickHouseRepository) queryMemoryMetricByInstance(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aMem := attrFloat(AttrSystemMemoryUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_mem,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jvm_mem,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_mem
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemMemoryUtilization, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJVMMemoryUsed, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		aMem, fmt.Sprintf("%.1f", PercentageThreshold), aMem, fmt.Sprintf("%.1f", PercentageMultiplier), aMem, aMem,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		aMem)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_mem"]),
		dbutil.Float64FromAny(row["jvm_mem"]),
		dbutil.Float64FromAny(row["attr_mem"]),
	}
	return calculateAverage(values), nil
}

// queryDiskMetricByInstance queries disk utilization metrics for a specific instance
func (r *ClickHouseRepository) queryDiskMetricByInstance(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aDisk := attrFloat(AttrSystemDiskUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_disk,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * (1.0 - (sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0))),
			   NULL) as ratio_disk,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_disk
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemDiskUtilization, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDiskFree, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		aDisk, fmt.Sprintf("%.1f", PercentageThreshold), aDisk, fmt.Sprintf("%.1f", PercentageMultiplier), aDisk, aDisk,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal,
		aDisk)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_disk"]),
		dbutil.Float64FromAny(row["ratio_disk"]),
		dbutil.Float64FromAny(row["attr_disk"]),
	}
	return calculateAverage(values), nil
}

// queryNetworkMetricByInstance queries network utilization metrics for a specific instance
func (r *ClickHouseRepository) queryNetworkMetricByInstance(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aNet := attrFloat(AttrSystemNetworkUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_net,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_net
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s = '%s'
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemNetworkUtilization, ColValue,
		aNet, fmt.Sprintf("%.1f", PercentageThreshold), aNet, fmt.Sprintf("%.1f", PercentageMultiplier), aNet, aNet,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemNetworkUtilization,
		aNet)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_net"]),
		dbutil.Float64FromAny(row["attr_net"]),
	}
	return calculateAverage(values), nil
}

// queryConnectionPoolMetricByInstance queries connection pool utilization metrics for a specific instance
func (r *ClickHouseRepository) queryConnectionPoolMetricByInstance(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as hikari_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jdbc_conn,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_conn
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR %s > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDBConnectionPoolUtilization, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricHikariCPConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJDBCConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		aConn, fmt.Sprintf("%.1f", PercentageThreshold), aConn, fmt.Sprintf("%.1f", PercentageMultiplier), aConn, aConn,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax, MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		aConn)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	values := []float64{
		dbutil.Float64FromAny(row["system_conn"]),
		dbutil.Float64FromAny(row["hikari_conn"]),
		dbutil.Float64FromAny(row["jdbc_conn"]),
		dbutil.Float64FromAny(row["attr_conn"]),
	}
	return calculateAverage(values), nil
}

// getSampleCountByInstance gets the count of metric samples for a specific instance
func (r *ClickHouseRepository) getSampleCountByInstance(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (int64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN (
		          '%s', '%s', '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s',
		          '%s', '%s'
		      )
		      OR %s > 0 OR %s > 0
		      OR %s > 0 OR %s > 0
		      OR %s > 0
		  )`,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName,
		MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal, MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		aCPU, aMem, aDisk, aNet, aConn)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row[ColCount]), nil
}

// getServiceList retrieves the list of distinct services that have metrics in the time range
func (r *ClickHouseRepository) getServiceList(teamID int64, startMs, endMs int64) ([]string, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s != ''
		  AND (
		      %s IN (
		          '%s', '%s', '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s',
		          '%s', '%s'
		      )
		      OR %s > 0 OR %s > 0
		      OR %s > 0 OR %s > 0
		      OR %s > 0
		  )
		ORDER BY %s`,
		ColServiceName,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColServiceName,
		ColMetricName,
		MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal, MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		aCPU, aMem, aDisk, aNet, aConn,
		ColServiceName)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = dbutil.StringFromAny(row[ColServiceName])
	}
	return services, nil
}

func (r *ClickHouseRepository) GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	aCPU := attrFloat(AttrSystemCPUUtilization)

	// Build CPU metric aggregation expression
	cpuSystemCol := fmt.Sprintf(`if(countIf(%s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) > 0, avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s), NULL)`,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold))
	cpuProcessCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) > 0, avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s), NULL)`,
		ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold))
	cpuAttrCol := fmt.Sprintf(`if(countIf(%s >= 0 AND %s <= %s) > 0, avgIf(%s * %s, %s >= 0 AND %s <= %s), NULL)`,
		aCPU, aCPU, fmt.Sprintf("%.1f", PercentageThreshold),
		aCPU, fmt.Sprintf("%.1f", PercentageMultiplier), aCPU, aCPU, fmt.Sprintf("%.1f", PercentageThreshold))
	cpuCol := syncAverageExpr(cpuSystemCol, cpuProcessCol, cpuAttrCol)

	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       %s as pod,
		       %s as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, bucket, ColServiceName, cpuCol, TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage,
		aCPU)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		buckets[i] = ResourceBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Pod:       dbutil.StringFromAny(row["pod"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	aMem := attrFloat(AttrSystemMemoryUtilization)

	// Build memory metric aggregation expression
	memSystemCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s)) > 0, avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)), NULL)`,
		ColMetricName, MetricSystemMemoryUtilization, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue,
		ColMetricName, MetricSystemMemoryUtilization, ColValue)
	memJvmCol := fmt.Sprintf(`if(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)) > 0, %s * sumIf(%s, %s = '%s' AND %s >= 0 AND isFinite(%s)) / nullIf(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)), 0), NULL)`,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageMultiplier),
		ColValue, ColMetricName, MetricJVMMemoryUsed, ColValue, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue, ColValue)
	memAttrCol := fmt.Sprintf(`if(countIf(%s > 0) > 0, avgIf(if(%s <= %s, %s * %s, %s), %s > 0), NULL)`,
		aMem, aMem, fmt.Sprintf("%.1f", PercentageThreshold),
		aMem, fmt.Sprintf("%.1f", PercentageMultiplier), aMem, aMem)
	memCol := syncAverageExpr(memSystemCol, memJvmCol, memAttrCol)

	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       %s as pod,
		       %s as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, bucket, ColServiceName, memCol, TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		aMem)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		buckets[i] = ResourceBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Pod:       dbutil.StringFromAny(row["pod"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetResourceUsageByService(teamID int64, startMs, endMs int64) ([]ServiceResource, error) {
	// Step 1: Get list of services
	services, err := r.getServiceList(teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	// Step 2: Query each metric for each service separately
	byService := make([]ServiceResource, len(services))
	for i, serviceName := range services {
		// Query CPU metric
		cpuVal, _ := r.queryCPUMetricByService(teamID, serviceName, startMs, endMs)

		// Query Memory metric
		memVal, _ := r.queryMemoryMetricByService(teamID, serviceName, startMs, endMs)

		// Query Disk metric
		diskVal, _ := r.queryDiskMetricByService(teamID, serviceName, startMs, endMs)

		// Query Network metric
		netVal, _ := r.queryNetworkMetricByService(teamID, serviceName, startMs, endMs)

		// Query Connection Pool metric
		connVal, _ := r.queryConnectionPoolMetricByService(teamID, serviceName, startMs, endMs)

		// Get sample count for this service
		sampleCount, _ := r.getSampleCountByService(teamID, serviceName, startMs, endMs)

		byService[i] = ServiceResource{
			ServiceName:           serviceName,
			AvgCpuUtil:            cpuVal,
			AvgMemoryUtil:         memVal,
			AvgDiskUtil:           diskVal,
			AvgNetworkUtil:        netVal,
			AvgConnectionPoolUtil: connVal,
			SampleCount:           sampleCount,
		}
	}

	return byService, nil
}

// getSampleCountByService gets the count of metric samples for a service
func (r *ClickHouseRepository) getSampleCountByService(teamID int64, serviceName string, startMs, endMs int64) (int64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN (
		          '%s', '%s', '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s',
		          '%s', '%s'
		      )
		      OR %s > 0 OR %s > 0
		      OR %s > 0 OR %s > 0
		      OR %s > 0
		  )`,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName,
		MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal, MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		aCPU, aMem, aDisk, aNet, aConn)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row[ColCount]), nil
}

func (r *ClickHouseRepository) GetResourceUsageByInstance(teamID int64, startMs, endMs int64) ([]InstanceResource, error) {
	// Step 1: Get list of instances (host, pod, container, service combinations)
	instances, err := r.getInstanceList(teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	// Step 2: Query each metric for each instance separately
	byInstance := make([]InstanceResource, len(instances))
	for i, inst := range instances {
		// Query CPU metric
		cpuVal, _ := r.queryCPUMetricByInstance(teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Memory metric
		memVal, _ := r.queryMemoryMetricByInstance(teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Disk metric
		diskVal, _ := r.queryDiskMetricByInstance(teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Network metric
		netVal, _ := r.queryNetworkMetricByInstance(teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Connection Pool metric
		connVal, _ := r.queryConnectionPoolMetricByInstance(teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Get sample count for this instance
		sampleCount, _ := r.getSampleCountByInstance(teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		byInstance[i] = InstanceResource{
			Host:                  inst.Host,
			Pod:                   inst.Pod,
			Container:             inst.Container,
			ServiceName:           inst.ServiceName,
			AvgCpuUtil:            cpuVal,
			AvgMemoryUtil:         memVal,
			AvgDiskUtil:           diskVal,
			AvgNetworkUtil:        netVal,
			AvgConnectionPoolUtil: connVal,
			SampleCount:           sampleCount,
		}
	}

	return byInstance, nil
}

// Instance represents a unique combination of host, pod, container, and service
type Instance struct {
	Host        string
	Pod         string
	Container   string
	ServiceName string
}

// getInstanceList retrieves the list of distinct instances that have metrics in the time range
func (r *ClickHouseRepository) getInstanceList(teamID int64, startMs, endMs int64) ([]Instance, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s, %s, %s, %s
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s != ''
		  AND (
		      %s IN (
		          '%s', '%s', '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s', '%s',
		          '%s', '%s', '%s',
		          '%s', '%s'
		      )
		      OR %s > 0 OR %s > 0
		      OR %s > 0 OR %s > 0
		      OR %s > 0
		  )
		ORDER BY %s, %s, %s, %s
		LIMIT 200`,
		ColHost, ColPod, ColContainer, ColServiceName,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColServiceName,
		ColMetricName,
		MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal, MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		aCPU, aMem, aDisk, aNet, aConn,
		ColHost, ColPod, ColContainer, ColServiceName)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	instances := make([]Instance, len(rows))
	for i, row := range rows {
		instances[i] = Instance{
			Host:        dbutil.StringFromAny(row[ColHost]),
			Pod:         dbutil.StringFromAny(row[ColPod]),
			Container:   dbutil.StringFromAny(row[ColContainer]),
			ServiceName: dbutil.StringFromAny(row[ColServiceName]),
		}
	}
	return instances, nil
}

// ─── System Infrastructure Metrics ───────────────────────────────────────────

func (r *ClickHouseRepository) GetCPUTime(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", AttrSystemCPUState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, state, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemCPUTime)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetMemoryUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, state, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemMemoryUsage)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetSwapUsage(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, state, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemPagingUsage)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", AttrSystemDiskDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, dir, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemDiskIO)
	return r.queryDirectionBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskOperations(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", AttrSystemDiskDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, dir, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemDiskOperations)
	return r.queryDirectionBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskIOTime(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		bucket, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemDiskIOTime)
	return r.queryResourceBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetFilesystemUsage(teamID int64, startMs, endMs int64) ([]MountpointBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	mp := fmt.Sprintf("attributes.'%s'::String", AttrFilesystemMountpoint)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as mountpoint, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, mp, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemFilesystemUsage)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]MountpointBucket, len(rows))
	for i, row := range rows {
		v := dbutil.NullableFloat64FromAny(row["metric_val"])
		buckets[i] = MountpointBucket{
			Timestamp:  dbutil.StringFromAny(row["time_bucket"]),
			Mountpoint: dbutil.StringFromAny(row["mountpoint"]),
			Value:      v,
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetFilesystemUtilization(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		bucket, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemFilesystemUtil)
	return r.queryResourceBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetNetworkIO(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", AttrSystemNetworkDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, dir, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemNetworkIO)
	return r.queryDirectionBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetNetworkPackets(teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", AttrSystemNetworkDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as direction, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, dir, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemNetworkPackets)
	return r.queryDirectionBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetNetworkErrors(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", AttrSystemNetworkDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, dir, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemNetworkErrors)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetNetworkDropped(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, sum(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		bucket, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemNetworkDropped)
	return r.queryResourceBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetLoadAverage(teamID int64, startMs, endMs int64) (LoadAverageResult, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s, %s = '%s') as load_1m,
			avgIf(%s, %s = '%s') as load_5m,
			avgIf(%s, %s = '%s') as load_15m
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s', '%s')`,
		ColValue, ColMetricName, MetricSystemCPULoadAvg1m,
		ColValue, ColMetricName, MetricSystemCPULoadAvg5m,
		ColValue, ColMetricName, MetricSystemCPULoadAvg15m,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricSystemCPULoadAvg1m, MetricSystemCPULoadAvg5m, MetricSystemCPULoadAvg15m)
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return LoadAverageResult{}, err
	}
	return LoadAverageResult{
		Load1m:  dbutil.Float64FromAny(row["load_1m"]),
		Load5m:  dbutil.Float64FromAny(row["load_5m"]),
		Load15m: dbutil.Float64FromAny(row["load_15m"]),
	}, nil
}

func (r *ClickHouseRepository) GetProcessCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	status := fmt.Sprintf("attributes.'%s'::String", AttrProcessStatus)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, status, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemProcessCount)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetNetworkConnections(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", AttrSystemNetworkState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, state, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricSystemNetworkConnections)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

// ─── JVM Runtime Metrics ─────────────────────────────────────────────────────

func (r *ClickHouseRepository) GetJVMMemory(teamID int64, startMs, endMs int64) ([]JVMMemoryBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", AttrJVMMemoryPoolName)
	memType := fmt.Sprintf("attributes.'%s'::String", AttrJVMMemoryType)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			%s as mem_type,
			avgIf(%s, %s = '%s') as used,
			avgIf(%s, %s = '%s') as committed,
			avgIf(%s, %s = '%s') as limit_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s', '%s')
		GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`,
		bucket, pool, memType,
		ColValue, ColMetricName, MetricJVMMemoryUsed,
		ColValue, ColMetricName, MetricJVMMemoryCommitted,
		ColValue, ColMetricName, MetricJVMMemoryLimit,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricJVMMemoryUsed, MetricJVMMemoryCommitted, MetricJVMMemoryLimit)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]JVMMemoryBucket, len(rows))
	for i, row := range rows {
		used := dbutil.NullableFloat64FromAny(row["used"])
		committed := dbutil.NullableFloat64FromAny(row["committed"])
		limit := dbutil.NullableFloat64FromAny(row["limit_val"])
		buckets[i] = JVMMemoryBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			PoolName:  dbutil.StringFromAny(row["pool_name"]),
			MemType:   dbutil.StringFromAny(row["mem_type"]),
			Used:      used,
			Committed: committed,
			Limit:     limit,
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetJVMGCDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
			quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) as p50,
			quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) as p95,
			quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) as p99,
			avg(hist_sum / nullIf(hist_count, 0)) as avg_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s = '%s' AND metric_type = 'Histogram'`,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricJVMGCDuration)
	return r.queryHistogramSummary(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMGCCollections(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as pod, sum(hist_count) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s = '%s' AND metric_type = 'Histogram'
		GROUP BY 1 ORDER BY 1`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricJVMGCDuration)
	return r.queryResourceBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMThreadCount(teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	daemon := fmt.Sprintf("attributes.'%s'::String", AttrJVMThreadDaemon)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ? AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, daemon, ColValue,
		TableMetrics,
		ColTeamID, ColTimestamp, ColMetricName, MetricJVMThreadCount)
	return r.queryStateBuckets(query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetJVMClasses(teamID int64, startMs, endMs int64) (JVMClassStats, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s') as loaded,
			avgIf(%s, %s = '%s') as count_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')`,
		ColValue, ColMetricName, MetricJVMClassLoaded,
		ColValue, ColMetricName, MetricJVMClassCount,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricJVMClassLoaded, MetricJVMClassCount)
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return JVMClassStats{}, err
	}
	return JVMClassStats{
		Loaded: dbutil.Int64FromAny(row["loaded"]),
		Count:  dbutil.Int64FromAny(row["count_val"]),
	}, nil
}

func (r *ClickHouseRepository) GetJVMCPU(teamID int64, startMs, endMs int64) (JVMCPUStats, error) {
	query := fmt.Sprintf(`
		SELECT
			sumIf(%s, %s = '%s') as cpu_time,
			avgIf(%s, %s = '%s') as cpu_util
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')`,
		ColValue, ColMetricName, MetricJVMCPUTime,
		ColValue, ColMetricName, MetricJVMCPUUtilization,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricJVMCPUTime, MetricJVMCPUUtilization)
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return JVMCPUStats{}, err
	}
	return JVMCPUStats{
		CPUTimeValue:      dbutil.Float64FromAny(row["cpu_time"]),
		RecentUtilization: dbutil.Float64FromAny(row["cpu_util"]),
	}, nil
}

func (r *ClickHouseRepository) GetJVMBuffers(teamID int64, startMs, endMs int64) ([]JVMBufferBucket, error) {
	bucket := resBucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", AttrJVMBufferPoolName)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			avgIf(%s, %s = '%s') as memory_usage,
			avgIf(%s, %s = '%s') as buf_count
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s')
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, pool,
		ColValue, ColMetricName, MetricJVMBufferMemoryUsage,
		ColValue, ColMetricName, MetricJVMBufferCount,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricJVMBufferMemoryUsage, MetricJVMBufferCount)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]JVMBufferBucket, len(rows))
	for i, row := range rows {
		mu := dbutil.NullableFloat64FromAny(row["memory_usage"])
		cnt := dbutil.NullableFloat64FromAny(row["buf_count"])
		buckets[i] = JVMBufferBucket{
			Timestamp:   dbutil.StringFromAny(row["time_bucket"]),
			PoolName:    dbutil.StringFromAny(row["pool_name"]),
			MemoryUsage: mu,
			Count:       cnt,
		}
	}
	return buckets, nil
}

// ─── Shared query helpers ─────────────────────────────────────────────────────

func (r *ClickHouseRepository) queryStateBuckets(query string, teamID int64, startMs, endMs int64) ([]StateBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]StateBucket, len(rows))
	for i, row := range rows {
		buckets[i] = StateBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			State:     dbutil.StringFromAny(row["state"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) queryDirectionBuckets(query string, teamID int64, startMs, endMs int64) ([]DirectionBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]DirectionBucket, len(rows))
	for i, row := range rows {
		buckets[i] = DirectionBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Direction: dbutil.StringFromAny(row["direction"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) queryResourceBuckets(query string, teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	buckets := make([]ResourceBucket, len(rows))
	for i, row := range rows {
		buckets[i] = ResourceBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Pod:       dbutil.StringFromAny(row["pod"]),
			Value:     dbutil.NullableFloat64FromAny(row["metric_val"]),
		}
	}
	return buckets, nil
}

func (r *ClickHouseRepository) queryHistogramSummary(query string, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return HistogramSummary{}, err
	}
	return HistogramSummary{
		P50: dbutil.Float64FromAny(row["p50"]),
		P95: dbutil.Float64FromAny(row["p95"]),
		P99: dbutil.Float64FromAny(row["p99"]),
		Avg: dbutil.Float64FromAny(row["avg_val"]),
	}, nil
}
