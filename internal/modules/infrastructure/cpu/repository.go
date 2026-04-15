package cpu

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

type Repository interface {
	GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (loadAverageResultDTO, error)
	GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetCPUByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuServiceMetricDTO, error)
	GetCPUByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuInstanceMetricDTO, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) Repository {
	return &ClickHouseRepository{db: db}
}

func bucket(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

func syncAverageExpr(parts ...string) string {
	joined := strings.Join(parts, ", ")
	return `if(
		length(arrayFilter(x -> isNotNull(x), [` + joined + `])) > 0,
		arrayReduce('avg', arrayFilter(x -> isNotNull(x), [` + joined + `])),
		NULL
	)`
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	var rows []stateBucketDTO
	if err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemCPUState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUTime)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)

	cpuSystemCol := fmt.Sprintf(`if(countIf(%s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) > 0, avgIf(%s * %.1f, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %.1f), NULL)`,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		infraconsts.ColValue, infraconsts.PercentageMultiplier,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold)
	cpuProcessCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) > 0, avgIf(%s * %.1f, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %.1f), NULL)`,
		infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		infraconsts.ColValue, infraconsts.PercentageMultiplier,
		infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage,
		infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold)
	cpuAttrCol := fmt.Sprintf(`if(countIf(%s >= 0 AND %s <= %.1f) > 0, avgIf(%s * %.1f, %s >= 0 AND %s <= %.1f), NULL)`,
		aCPU, aCPU, infraconsts.PercentageThreshold,
		aCPU, infraconsts.PercentageMultiplier, aCPU, aCPU, infraconsts.PercentageThreshold)
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
	`, b, infraconsts.ColServiceName, cpuCol,
		infraconsts.TableMetrics, infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)
	var rows []resourceBucketDTO
	if err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (loadAverageResultDTO, error) {
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s, %s = '%s') as load_1m,
			avgIf(%s, %s = '%s') as load_5m,
			avgIf(%s, %s = '%s') as load_15m
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s IN ('%s', '%s', '%s')`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg1m,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg5m,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg15m,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPULoadAvg1m, infraconsts.MetricSystemCPULoadAvg5m, infraconsts.MetricSystemCPULoadAvg15m)
	var result loadAverageResultDTO
	if err := r.db.QueryRow(ctx, &result, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return loadAverageResultDTO{}, err
	}
	return result, nil
}

func (r *ClickHouseRepository) GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	status := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrProcessStatus)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, status, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemProcessCount)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

// DTO for queries returning multiple computed metric columns.
type cpuMetricRow struct {
	SystemCPU  *float64 `ch:"system_cpu"`
	ProcessCPU *float64 `ch:"process_cpu"`
	AttrCPU    *float64 `ch:"attr_cpu"`
}

type serviceNameRow struct {
	ServiceName string `ch:"service_name"`
}

type instanceRow struct {
	Host        string `ch:"host"`
	Pod         string `ch:"pod"`
	Container   string `ch:"container"`
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

// calculateAverage computes the mean of valid (non-NaN, non-Inf, non-negative) values.
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

func (r *ClickHouseRepository) queryCPUMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %.1f, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) as system_cpu,
			avgIf(%s * %.1f, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) as process_cpu,
			avgIf(%s * %.1f, %s >= 0 AND %s <= %.1f) as attr_cpu
		FROM %s
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage, infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		aCPU, infraconsts.PercentageMultiplier, aCPU, aCPU, infraconsts.PercentageThreshold,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)

	var row cpuMetricRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemCPU, row.ProcessCPU, row.AttrCPU)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryCPUMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(%s * %.1f, %s IN ('%s', '%s') AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) as system_cpu,
			avgIf(%s * %.1f, %s = '%s' AND isFinite(%s) AND %s >= 0 AND %s <= %.1f) as process_cpu,
			avgIf(%s * %.1f, %s >= 0 AND %s <= %.1f) as attr_cpu
		FROM %s
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )`,
		infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage, infraconsts.ColValue, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageThreshold,
		aCPU, infraconsts.PercentageMultiplier, aCPU, aCPU, infraconsts.PercentageThreshold,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColHost, infraconsts.ColPod, infraconsts.ColContainer, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)

	var row cpuMetricRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemCPU, row.ProcessCPU, row.AttrCPU)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s != ''
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )
		ORDER BY service_name`,
		infraconsts.ColServiceName,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColServiceName,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)

	var rows []serviceNameRow
	err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = row.ServiceName
	}
	return services, nil
}

func (r *ClickHouseRepository) getInstanceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]instanceRow, error) {
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as host, %s as pod, %s as container, %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s != ''
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )
		LIMIT 200`,
		infraconsts.ColHost, infraconsts.ColPod, infraconsts.ColContainer, infraconsts.ColServiceName,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColServiceName,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)

	var rows []instanceRow
	err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
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

func (r *ClickHouseRepository) GetCPUByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuServiceMetricDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	result := make([]cpuServiceMetricDTO, len(services))
	for i, serviceName := range services {
		cpuVal, _ := r.queryCPUMetricByService(ctx, teamID, serviceName, startMs, endMs)
		result[i] = cpuServiceMetricDTO{
			ServiceName: serviceName,
			Value:       cpuVal,
		}
	}
	return result, nil
}

func (r *ClickHouseRepository) GetCPUByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuInstanceMetricDTO, error) {
	instances, err := r.getInstanceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	result := make([]cpuInstanceMetricDTO, len(instances))
	for i, inst := range instances {
		cpuVal, _ := r.queryCPUMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		result[i] = cpuInstanceMetricDTO{
			Host:        inst.Host,
			Pod:         inst.Pod,
			Container:   inst.Container,
			ServiceName: inst.ServiceName,
			Value:       cpuVal,
		}
	}
	return result, nil
}
