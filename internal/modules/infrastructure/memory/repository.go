package memory

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

const metricsGaugesRollupPrefix = "observability.metrics_gauges_rollup"

// queryIntervalMinutes returns the group-by step (in minutes) for rollup reads.
// It is max(tierStep, dashboardStep) so the step is never finer than the
// selected tier's native resolution.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	if tierStepMin > dashStep {
		return tierStepMin
	}
	return dashStep
}

type Repository interface {
	GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error)
	GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func bucket(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

// syncAverageExpr builds an avg-of-non-null expression over the given column
// expressions without arrayReduce/arrayFilter. Pure per-row expression, same
// NULL-drop / empty-set-→-NULL semantics as the prior arrayReduce formulation.
func syncAverageExpr(parts ...string) string {
	sum := make([]string, len(parts))
	cnt := make([]string, len(parts))
	for i, p := range parts {
		sum[i] = "coalesce(" + p + ", 0)"
		cnt[i] = "if(" + p + " IS NULL, 0, 1)"
	}
	return "(" + strings.Join(sum, " + ") + ") / nullIf(" + strings.Join(cnt, " + ") + ", 0)"
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	var rows []stateBucketDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       state_dim                AS state,
		       sumMerge(value_avg_num)  AS value_num,
		       sumMerge(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND state_dim != ''
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemMemoryUsage),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		State     string    `ch:"state"`
		ValueNum  float64   `ch:"value_num"`
		ValueCnt  uint64    `ch:"value_cnt"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]stateBucketDTO, len(raw))
	for i, row := range raw {
		var valPtr *float64
		if row.ValueCnt > 0 {
			v := row.ValueNum / float64(row.ValueCnt)
			valPtr = &v
		}
		rows[i] = StateBucket{
			Timestamp: row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			State:     row.State,
			Value:     valPtr,
		}
	}
	return rows, nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
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
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
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
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
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
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, serviceParams(teamID, serviceName, startMs, endMs)...).ScanStruct(&row)
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
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...).ScanStruct(&row)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemMem, row.JVMMem, row.AttrMem)
	return calculateAverage(values), nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
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

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryMemoryMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryMemoryMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
