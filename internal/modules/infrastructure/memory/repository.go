package memory

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
	GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error)
	GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error)
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

func (r *ClickHouseRepository) GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUsage)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)

	memSystemCol := fmt.Sprintf(`if(countIf(%s = '%s' AND isFinite(%s)) > 0, avgIf(if(%s <= %.1f, %s * %.1f, %s), %s = '%s' AND isFinite(%s)), NULL)`,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.PercentageThreshold, infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColValue,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.ColValue)
	memJvmCol := fmt.Sprintf(`if(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)) > 0, %.1f * sumIf(%s, %s = '%s' AND %s >= 0 AND isFinite(%s)) / nullIf(sumIf(%s, %s = '%s' AND %s > 0 AND isFinite(%s)), 0), NULL)`,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue, infraconsts.ColValue, infraconsts.PercentageMultiplier,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.ColValue, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue, infraconsts.ColValue)
	memAttrCol := fmt.Sprintf(`if(countIf(%s > 0) > 0, avgIf(if(%s <= %.1f, %s * %.1f, %s), %s > 0), NULL)`,
		aMem, aMem, infraconsts.PercentageThreshold,
		aMem, infraconsts.PercentageMultiplier, aMem, aMem)
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
	`, b, infraconsts.ColServiceName, memCol,
		infraconsts.TableMetrics, infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
		aMem)
	var rows []resourceBucketDTO
	if err := r.db.Select(ctx, &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemPagingUsage)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

type memMetricRow struct {
	SystemMem *float64 `ch:"system_mem"`
	JVMMem    *float64 `ch:"jvm_mem"`
	AttrMem   *float64 `ch:"attr_mem"`
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

func nullableToSlice(ptrs ...*float64) []float64 {
	out := make([]float64, 0, len(ptrs))
	for _, p := range ptrs {
		if p != nil && !math.IsNaN(*p) && !math.IsInf(*p, 0) {
			out = append(out, *p)
		}
	}
	return out
}

// Column expressions matching resourceutil for instance-level queries
const (
	colHostCoalesce      = "coalesce(nullIf(host, ''), nullIf(attributes.`host.name`::String, ''), nullIf(attributes.`server.address`::String, ''), 'unknown')"
	colPodCoalesce       = "coalesce(nullIf(attributes.`k8s.pod.name`::String, ''), '')"
	colContainerCoalesce = "coalesce(nullIf(attributes.`container.name`::String, ''), nullIf(attributes.`k8s.container.name`::String, ''), '')"
)

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)
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
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
		aMem)

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

func (r *ClickHouseRepository) queryMemoryMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)
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
		infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue,
		fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue,
		aMem, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), aMem, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), aMem, aMem,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
		aMem)

	var row memMetricRow
	err := r.db.QueryRow(ctx, &row, query, serviceParams(teamID, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemMem, row.JVMMem, row.AttrMem)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryMemoryMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)
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
		infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue,
		fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJVMMemoryMax, infraconsts.ColValue,
		aMem, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), aMem, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), aMem, aMem,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, colHostCoalesce, colPodCoalesce, colContainerCoalesce, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
		aMem)

	var row memMetricRow
	err := r.db.QueryRow(ctx, &row, query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemMem, row.JVMMem, row.AttrMem)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, svc := range services {
		memVal, err := r.queryMemoryMetricByService(ctx, teamID, svc, startMs, endMs)
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

func (r *ClickHouseRepository) GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryMemoryMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

func (r *ClickHouseRepository) GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryMemoryMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
