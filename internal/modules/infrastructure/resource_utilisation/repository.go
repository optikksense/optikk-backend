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
	GetAvgCPU(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgMemory(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgNetwork(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetAvgConnPool(teamUUID string, startMs, endMs int64) (MetricValue, error)
	GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error)
	GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]ServiceResource, error)
	GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]InstanceResource, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new Resource Utilization Repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

// queryDiskMetricByService queries disk utilization metrics for a service
func (r *ClickHouseRepository) queryDiskMetricByService(teamUUID, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_disk,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * (1.0 - (sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0))),
			   NULL) as ratio_disk,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_disk
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemDiskUtilization, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDiskFree, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		ColAttributes, AttrSystemDiskUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrSystemDiskUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemDiskUtilization,
		ColAttributes, AttrSystemDiskUtilization,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal,
		ColAttributes, AttrSystemDiskUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryNetworkMetricByService(teamUUID, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_net,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_net
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s = '%s'
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemNetworkUtilization, ColValue,
		ColAttributes, AttrSystemNetworkUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrSystemNetworkUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemNetworkUtilization,
		ColAttributes, AttrSystemNetworkUtilization,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemNetworkUtilization,
		ColAttributes, AttrSystemNetworkUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryConnectionPoolMetricByService(teamUUID, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as hikari_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jdbc_conn,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_conn
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDBConnectionPoolUtilization, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricHikariCPConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJDBCConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		ColAttributes, AttrDBConnectionPoolUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrDBConnectionPoolUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrDBConnectionPoolUtilization,
		ColAttributes, AttrDBConnectionPoolUtilization,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax, MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		ColAttributes, AttrDBConnectionPoolUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryCPUMetricByService(teamUUID, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) as system_cpu,
			avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) as process_cpu,
			avgIf(JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s') >= 0 AND JSONExtractFloat(%s, '%s') <= %s) as attr_cpu
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColAttributes, AttrSystemCPUUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemCPUUtilization, fmt.Sprintf("%.1f", PercentageThreshold),
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage,
		ColAttributes, AttrSystemCPUUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryMemoryMetricByService(teamUUID, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_mem,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jvm_mem,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_mem
		FROM %s
		WHERE %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemMemoryUtilization, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJVMMemoryUsed, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		ColAttributes, AttrSystemMemoryUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrSystemMemoryUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemMemoryUtilization,
		ColAttributes, AttrSystemMemoryUtilization,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		ColAttributes, AttrSystemMemoryUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetAvgCPU(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamUUID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query CPU metric for each service and calculate average
	var values []float64
	for _, service := range services {
		cpuVal, err := r.queryCPUMetricByService(teamUUID, service, startMs, endMs)
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

func (r *ClickHouseRepository) GetAvgMemory(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamUUID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query memory metric for each service and calculate average
	var values []float64
	for _, service := range services {
		memVal, err := r.queryMemoryMetricByService(teamUUID, service, startMs, endMs)
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

func (r *ClickHouseRepository) GetAvgNetwork(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamUUID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query network metric for each service and calculate average
	var values []float64
	for _, service := range services {
		netVal, err := r.queryNetworkMetricByService(teamUUID, service, startMs, endMs)
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

func (r *ClickHouseRepository) GetAvgConnPool(teamUUID string, startMs, endMs int64) (MetricValue, error) {
	// Get list of services
	services, err := r.getServiceList(teamUUID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	// Query connection pool metric for each service and calculate average
	var values []float64
	for _, service := range services {
		connVal, err := r.queryConnectionPoolMetricByService(teamUUID, service, startMs, endMs)
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
func (r *ClickHouseRepository) queryCPUMetricByInstance(teamUUID, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) as system_cpu,
			avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) as process_cpu,
			avgIf(JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s') >= 0 AND JSONExtractFloat(%s, '%s') <= %s) as attr_cpu
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColAttributes, AttrSystemCPUUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemCPUUtilization, fmt.Sprintf("%.1f", PercentageThreshold),
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage,
		ColAttributes, AttrSystemCPUUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryMemoryMetricByInstance(teamUUID, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_mem,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jvm_mem,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_mem
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemMemoryUtilization, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJVMMemoryUsed, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue,
		ColAttributes, AttrSystemMemoryUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrSystemMemoryUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemMemoryUtilization,
		ColAttributes, AttrSystemMemoryUtilization,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		ColAttributes, AttrSystemMemoryUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryDiskMetricByInstance(teamUUID, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_disk,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * (1.0 - (sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0))),
			   NULL) as ratio_disk,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_disk
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemDiskUtilization, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDiskFree, ColValue,
		ColValue, ColMetricName, MetricDiskTotal, ColValue,
		ColAttributes, AttrSystemDiskUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrSystemDiskUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemDiskUtilization,
		ColAttributes, AttrSystemDiskUtilization,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal,
		ColAttributes, AttrSystemDiskUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryNetworkMetricByInstance(teamUUID, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_net,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_net
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s = '%s'
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricSystemNetworkUtilization, ColValue,
		ColAttributes, AttrSystemNetworkUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrSystemNetworkUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemNetworkUtilization,
		ColAttributes, AttrSystemNetworkUtilization,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricSystemNetworkUtilization,
		ColAttributes, AttrSystemNetworkUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) queryConnectionPoolMetricByInstance(teamUUID, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as hikari_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jdbc_conn,
			avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')),
			      JSONExtractFloat(%s, '%s') > 0) as attr_conn
		FROM %s
		WHERE %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricDBConnectionPoolUtilization, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricHikariCPConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricHikariCPConnectionsMax, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		fmt.Sprintf("%.1f", PercentageMultiplier), ColValue, ColMetricName, MetricJDBCConnectionsActive, ColValue,
		ColValue, ColMetricName, MetricJDBCConnectionsMax, ColValue,
		ColAttributes, AttrDBConnectionPoolUtilization, fmt.Sprintf("%.1f", PercentageThreshold), ColAttributes, AttrDBConnectionPoolUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrDBConnectionPoolUtilization,
		ColAttributes, AttrDBConnectionPoolUtilization,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName, MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax, MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		ColAttributes, AttrDBConnectionPoolUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
func (r *ClickHouseRepository) getSampleCountByInstance(teamUUID, host, pod, container, serviceName string, startMs, endMs int64) (int64, error) {
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
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		TableMetrics,
		ColTeamID, ColHost, ColPod, ColContainer, ColServiceName, ColTimestamp,
		ColMetricName,
		MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal, MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemMemoryUtilization,
		ColAttributes, AttrSystemDiskUtilization, ColAttributes, AttrSystemNetworkUtilization,
		ColAttributes, AttrDBConnectionPoolUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, host, pod, container, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row[ColCount]), nil
}

// getServiceList retrieves the list of distinct services that have metrics in the time range
func (r *ClickHouseRepository) getServiceList(teamUUID string, startMs, endMs int64) ([]string, error) {
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
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0
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
		ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemMemoryUtilization,
		ColAttributes, AttrSystemDiskUtilization, ColAttributes, AttrSystemNetworkUtilization,
		ColAttributes, AttrDBConnectionPoolUtilization,
		ColServiceName)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = dbutil.StringFromAny(row[ColServiceName])
	}
	return services, nil
}

func (r *ClickHouseRepository) GetCPUUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)

	// Build CPU metric aggregation expression
	cpuSystemCol := fmt.Sprintf(`if(countIf(%s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) > 0, avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s), NULL)`,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold))
	cpuProcessCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) > 0, avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s), NULL)`,
		ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold),
		ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColMetricName, MetricProcessCPUUsage, ColValue, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold))
	cpuAttrCol := fmt.Sprintf(`if(countIf(JSONExtractFloat(%s, '%s') >= 0 AND JSONExtractFloat(%s, '%s') <= %s) > 0, avgIf(JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s') >= 0 AND JSONExtractFloat(%s, '%s') <= %s), NULL)`,
		ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemCPUUtilization, fmt.Sprintf("%.1f", PercentageThreshold),
		ColAttributes, AttrSystemCPUUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemCPUUtilization, fmt.Sprintf("%.1f", PercentageThreshold))
	cpuCol := syncAverageExpr(cpuSystemCol, cpuProcessCol, cpuAttrCol)

	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       %s as pod,
		       %s as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, bucket, ColServiceName, cpuCol, TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage,
		ColAttributes, AttrSystemCPUUtilization)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetMemoryUsagePercentage(teamUUID string, startMs, endMs int64) ([]ResourceBucket, error) {
	bucket := resBucketExpr(startMs, endMs)

	// Build memory metric aggregation expression
	memSystemCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s)) > 0, avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)), NULL)`,
		ColMetricName, MetricSystemMemoryUtilization, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageThreshold), ColValue, fmt.Sprintf("%.1f", PercentageMultiplier), ColValue,
		ColMetricName, MetricSystemMemoryUtilization, ColValue)
	memJvmCol := fmt.Sprintf(`if(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)) > 0, %s * sumIf(%s, %s = '%s' AND %s >= 0 AND isFinite(%s)) / nullIf(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)), 0), NULL)`,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue, ColValue, fmt.Sprintf("%.1f", PercentageMultiplier),
		ColValue, ColMetricName, MetricJVMMemoryUsed, ColValue, ColValue,
		ColValue, ColMetricName, MetricJVMMemoryMax, ColValue, ColValue)
	memAttrCol := fmt.Sprintf(`if(countIf(JSONExtractFloat(%s, '%s') > 0) > 0, avgIf(if(JSONExtractFloat(%s, '%s') <= %s, JSONExtractFloat(%s, '%s') * %s, JSONExtractFloat(%s, '%s')), JSONExtractFloat(%s, '%s') > 0), NULL)`,
		ColAttributes, AttrSystemMemoryUtilization, ColAttributes, AttrSystemMemoryUtilization, fmt.Sprintf("%.1f", PercentageThreshold),
		ColAttributes, AttrSystemMemoryUtilization, fmt.Sprintf("%.1f", PercentageMultiplier), ColAttributes, AttrSystemMemoryUtilization, ColAttributes, AttrSystemMemoryUtilization)
	memCol := syncAverageExpr(memSystemCol, memJvmCol, memAttrCol)

	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       %s as pod,
		       %s as metric_val
		FROM %s
		WHERE %s = ? AND %s BETWEEN ? AND ?
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR JSONExtractFloat(%s, '%s') > 0
		  )
		GROUP BY 1, 2
		HAVING pod != ''
		ORDER BY 1 ASC, 2 ASC
	`, bucket, ColServiceName, memCol, TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		ColAttributes, AttrSystemMemoryUtilization)
	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

func (r *ClickHouseRepository) GetResourceUsageByService(teamUUID string, startMs, endMs int64) ([]ServiceResource, error) {
	// Step 1: Get list of services
	services, err := r.getServiceList(teamUUID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	// Step 2: Query each metric for each service separately
	byService := make([]ServiceResource, len(services))
	for i, serviceName := range services {
		// Query CPU metric
		cpuVal, _ := r.queryCPUMetricByService(teamUUID, serviceName, startMs, endMs)

		// Query Memory metric
		memVal, _ := r.queryMemoryMetricByService(teamUUID, serviceName, startMs, endMs)

		// Query Disk metric
		diskVal, _ := r.queryDiskMetricByService(teamUUID, serviceName, startMs, endMs)

		// Query Network metric
		netVal, _ := r.queryNetworkMetricByService(teamUUID, serviceName, startMs, endMs)

		// Query Connection Pool metric
		connVal, _ := r.queryConnectionPoolMetricByService(teamUUID, serviceName, startMs, endMs)

		// Get sample count for this service
		sampleCount, _ := r.getSampleCountByService(teamUUID, serviceName, startMs, endMs)

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
func (r *ClickHouseRepository) getSampleCountByService(teamUUID, serviceName string, startMs, endMs int64) (int64, error) {
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
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0
		  )`,
		TableMetrics,
		ColTeamID, ColServiceName, ColTimestamp,
		ColMetricName,
		MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal, MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemMemoryUtilization,
		ColAttributes, AttrSystemDiskUtilization, ColAttributes, AttrSystemNetworkUtilization,
		ColAttributes, AttrDBConnectionPoolUtilization)

	row, err := dbutil.QueryMap(r.db, query, teamUUID, serviceName, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row[ColCount]), nil
}

func (r *ClickHouseRepository) GetResourceUsageByInstance(teamUUID string, startMs, endMs int64) ([]InstanceResource, error) {
	// Step 1: Get list of instances (host, pod, container, service combinations)
	instances, err := r.getInstanceList(teamUUID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	// Step 2: Query each metric for each instance separately
	byInstance := make([]InstanceResource, len(instances))
	for i, inst := range instances {
		// Query CPU metric
		cpuVal, _ := r.queryCPUMetricByInstance(teamUUID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Memory metric
		memVal, _ := r.queryMemoryMetricByInstance(teamUUID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Disk metric
		diskVal, _ := r.queryDiskMetricByInstance(teamUUID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Network metric
		netVal, _ := r.queryNetworkMetricByInstance(teamUUID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Query Connection Pool metric
		connVal, _ := r.queryConnectionPoolMetricByInstance(teamUUID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

		// Get sample count for this instance
		sampleCount, _ := r.getSampleCountByInstance(teamUUID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

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
func (r *ClickHouseRepository) getInstanceList(teamUUID string, startMs, endMs int64) ([]Instance, error) {
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
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0 OR JSONExtractFloat(%s, '%s') > 0
		      OR JSONExtractFloat(%s, '%s') > 0
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
		ColAttributes, AttrSystemCPUUtilization, ColAttributes, AttrSystemMemoryUtilization,
		ColAttributes, AttrSystemDiskUtilization, ColAttributes, AttrSystemNetworkUtilization,
		ColAttributes, AttrDBConnectionPoolUtilization,
		ColHost, ColPod, ColContainer, ColServiceName)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
