package resource_utilisation //nolint:misspell

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
)

func resBucketExpr(startMs, endMs int64) string {
	return TimeBucketExpression(startMs, endMs)
}

type Repository interface {
	GetAvgCPU(teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetAvgMemory(teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetAvgNetwork(teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetAvgConnPool(teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetResourceUsageByService(teamID int64, startMs, endMs int64) ([]serviceResourceDTO, error)
	GetResourceUsageByInstance(teamID int64, startMs, endMs int64) ([]instanceResourceDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) Repository {
	return &ClickHouseRepository{db: db}
}

// DTO for queries returning multiple computed metric columns.
type cpuMetricRow struct {
	SystemCPU  *float64 `ch:"system_cpu"`
	ProcessCPU *float64 `ch:"process_cpu"`
	AttrCPU    *float64 `ch:"attr_cpu"`
}

type memMetricRow struct {
	SystemMem *float64 `ch:"system_mem"`
	JVMMem    *float64 `ch:"jvm_mem"`
	AttrMem   *float64 `ch:"attr_mem"`
}

type diskMetricRow struct {
	SystemDisk *float64 `ch:"system_disk"`
	RatioDisk  *float64 `ch:"ratio_disk"`
	AttrDisk   *float64 `ch:"attr_disk"`
}

type netMetricRow struct {
	SystemNet *float64 `ch:"system_net"`
	AttrNet   *float64 `ch:"attr_net"`
}

type connMetricRow struct {
	SystemConn *float64 `ch:"system_conn"`
	HikariConn *float64 `ch:"hikari_conn"`
	JDBCConn   *float64 `ch:"jdbc_conn"`
	AttrConn   *float64 `ch:"attr_conn"`
}

type countRow struct {
	Count int64 `ch:"count"`
}

type serviceNameRow struct {
	ServiceName string `ch:"service_name"`
}

func serviceParams(teamID int64, serviceName string, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func instanceParams(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("container", container),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (r *ClickHouseRepository) queryDiskMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aDisk := attrFloat(AttrSystemDiskUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_disk,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * (1.0 - (sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0))),
			   NULL) as ratio_disk,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_disk
		FROM %s
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row diskMetricRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemDisk, row.RatioDisk, row.AttrDisk)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryNetworkMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aNet := attrFloat(AttrSystemNetworkUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_net,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_net
		FROM %s
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row netMetricRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemNet, row.AttrNet)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryConnectionPoolMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
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
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row connMetricRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemConn, row.HikariConn, row.JDBCConn, row.AttrConn)
	return calculateAverage(values), nil
}

// Helper function to calculate average from non-null values
func calculateAverage(values []float64) *float64 {
	var sum float64
	count := 0
	for _, v := range values {
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
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

// nullableToSlice converts nullable float64 pointers to a []float64 of non-nil values.
func nullableToSlice(ptrs ...*float64) []float64 {
	out := make([]float64, 0, len(ptrs))
	for _, p := range ptrs {
		if p != nil && !math.IsNaN(*p) && !math.IsInf(*p, 0) {
			out = append(out, *p)
		}
	}
	return out
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

func (r *ClickHouseRepository) queryCPUMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) as system_cpu,
			avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) as process_cpu,
			avgIf(%s * %s, %s >= 0 AND %s <= %s) as attr_cpu
		FROM %s
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row cpuMetricRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemCPU, row.ProcessCPU, row.AttrCPU)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryMemoryMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aMem := attrFloat(AttrSystemMemoryUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_mem,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jvm_mem,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_mem
		FROM %s
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row memMetricRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemMem, row.JVMMem, row.AttrMem)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) GetAvgCPU(teamID int64, startMs, endMs int64) (MetricValue, error) {
	ctx := context.Background()
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, service := range services {
		cpuVal, err := r.queryCPUMetricByService(ctx, teamID, service, startMs, endMs)
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
	ctx := context.Background()
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, service := range services {
		memVal, err := r.queryMemoryMetricByService(ctx, teamID, service, startMs, endMs)
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
	ctx := context.Background()
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, service := range services {
		netVal, err := r.queryNetworkMetricByService(ctx, teamID, service, startMs, endMs)
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
	ctx := context.Background()
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, service := range services {
		connVal, err := r.queryConnectionPoolMetricByService(ctx, teamID, service, startMs, endMs)
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

func (r *ClickHouseRepository) queryCPUMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %s, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %s) as system_cpu,
			avgIf(%s * %s, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %s) as process_cpu,
			avgIf(%s * %s, %s >= 0 AND %s <= %s) as attr_cpu
		FROM %s
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row cpuMetricRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemCPU, row.ProcessCPU, row.AttrCPU)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryMemoryMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aMem := attrFloat(AttrSystemMemoryUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_mem,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jvm_mem,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_mem
		FROM %s
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row memMetricRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemMem, row.JVMMem, row.AttrMem)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryDiskMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aDisk := attrFloat(AttrSystemDiskUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_disk,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %s * (1.0 - (sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0))),
			   NULL) as ratio_disk,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_disk
		FROM %s
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row diskMetricRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemDisk, row.RatioDisk, row.AttrDisk)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryNetworkMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aNet := attrFloat(AttrSystemNetworkUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %s, %s * %s, %s), %s = '%s' AND isFinite(%s)) as system_net,
			avgIf(if(%s <= %s, %s * %s, %s), %s > 0) as attr_net
		FROM %s
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row netMetricRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemNet, row.AttrNet)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryConnectionPoolMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
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
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row connMetricRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemConn, row.HikariConn, row.JDBCConn, row.AttrConn)
	return calculateAverage(values), nil
}

// getSampleCountByInstance gets the count of metric samples for a specific instance
func (r *ClickHouseRepository) getSampleCountByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (int64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT toInt64(COUNT(*)) as count
		FROM %s
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row countRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return 0, err
	}
	return row.Count, nil
}

// getServiceList retrieves the list of distinct services that have metrics in the time range
func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
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
		ORDER BY service_name`,
		ColServiceName,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColServiceName,
		ColMetricName,
		MetricSystemCPUUtilization, MetricSystemCPUUsage, MetricProcessCPUUsage, MetricSystemMemoryUtilization, MetricJVMMemoryUsed, MetricJVMMemoryMax,
		MetricSystemDiskUtilization, MetricDiskFree, MetricDiskTotal, MetricSystemNetworkUtilization,
		MetricDBConnectionPoolUtilization, MetricHikariCPConnectionsActive, MetricHikariCPConnectionsMax,
		MetricJDBCConnectionsActive, MetricJDBCConnectionsMax,
		aCPU, aMem, aDisk, aNet, aConn)

	var rows []serviceNameRow
	err := r.db.Select(ctx, &rows, query, database.SimpleBaseParams(teamID, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = row.ServiceName
	}
	return services, nil
}

func (r *ClickHouseRepository) GetCPUUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	ctx := context.Background()
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
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
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
	var rows []ResourceBucket
	err := r.db.Select(ctx, &rows, query, database.SimpleBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetMemoryUsagePercentage(teamID int64, startMs, endMs int64) ([]ResourceBucket, error) {
	ctx := context.Background()
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
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
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
	var rows []ResourceBucket
	err := r.db.Select(ctx, &rows, query, database.SimpleBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetResourceUsageByService(teamID int64, startMs, endMs int64) ([]ServiceResource, error) {
	ctx := context.Background()
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	byService := make([]ServiceResource, len(services))
	for i, serviceName := range services {
		cpuVal, _ := r.queryCPUMetricByService(ctx, teamID, serviceName, startMs, endMs)
		memVal, _ := r.queryMemoryMetricByService(ctx, teamID, serviceName, startMs, endMs)
		diskVal, _ := r.queryDiskMetricByService(ctx, teamID, serviceName, startMs, endMs)
		netVal, _ := r.queryNetworkMetricByService(ctx, teamID, serviceName, startMs, endMs)
		connVal, _ := r.queryConnectionPoolMetricByService(ctx, teamID, serviceName, startMs, endMs)
		sampleCount, _ := r.getSampleCountByService(ctx, teamID, serviceName, startMs, endMs)

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
func (r *ClickHouseRepository) getSampleCountByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (int64, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT toInt64(COUNT(*)) as count
		FROM %s
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
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

	var row countRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return 0, err
	}
	return row.Count, nil
}

func (r *ClickHouseRepository) GetResourceUsageByInstance(teamID int64, startMs, endMs int64) ([]InstanceResource, error) {
	ctx := context.Background()
	instances, err := r.getInstanceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	byInstance := make([]InstanceResource, len(instances))
	for i, inst := range instances {
		cpuVal, _ := r.queryCPUMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		memVal, _ := r.queryMemoryMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		diskVal, _ := r.queryDiskMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		netVal, _ := r.queryNetworkMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		connVal, _ := r.queryConnectionPoolMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		sampleCount, _ := r.getSampleCountByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)

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

// Instance holds distinct host/pod/container/service combinations.
type Instance struct {
	Host        string `ch:"host"`
	Pod         string `ch:"pod"`
	Container   string `ch:"container"`
	ServiceName string `ch:"service_name"`
}

// getInstanceList retrieves the list of distinct instances that have metrics in the time range
func (r *ClickHouseRepository) getInstanceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]Instance, error) {
	aCPU := attrFloat(AttrSystemCPUUtilization)
	aMem := attrFloat(AttrSystemMemoryUtilization)
	aDisk := attrFloat(AttrSystemDiskUtilization)
	aNet := attrFloat(AttrSystemNetworkUtilization)
	aConn := attrFloat(AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as host, %s as pod, %s as container, %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
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
		aCPU, aMem, aDisk, aNet, aConn)

	var rows []Instance
	err := r.db.Select(ctx, &rows, query, database.SimpleBaseParams(teamID, startMs, endMs)...)
	return rows, err
}
