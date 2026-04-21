package disk

import (
	"context"
	"fmt"
	"math"
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
	GetDiskIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error)
	GetDiskOperations(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error)
	GetDiskIOTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]mountpointBucketDTO, error)
	GetFilesystemUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetAvgDisk(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetDiskByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error)
	GetDiskByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error)
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

func (r *ClickHouseRepository) queryDirectionBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	var rows []directionBucketDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) queryResourceBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	var rows []resourceBucketDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) queryDirectionBucketsFromRollup(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]directionBucketDTO, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       state_dim                AS direction,
		       sumMerge(value_sum)      AS value_sum_val,
		       sumMerge(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND state_dim != ''
		GROUP BY time_bucket, direction
		ORDER BY time_bucket, direction`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		Direction string    `ch:"direction"`
		ValueSum  float64   `ch:"value_sum_val"`
		ValueCnt  uint64    `ch:"value_cnt"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]directionBucketDTO, len(raw))
	for i, row := range raw {
		var valPtr *float64
		if row.ValueCnt > 0 {
			v := row.ValueSum
			valPtr = &v
		}
		rows[i] = DirectionBucket{
			Timestamp: row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			Direction: row.Direction,
			Value:     valPtr,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetDiskIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBucketsFromRollup(ctx, teamID, startMs, endMs, infraconsts.MetricSystemDiskIO)
}

func (r *ClickHouseRepository) GetDiskOperations(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBucketsFromRollup(ctx, teamID, startMs, endMs, infraconsts.MetricSystemDiskOperations)
}

func (r *ClickHouseRepository) GetDiskIOTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       sumMerge(value_sum)      AS value_sum_val,
		       sumMerge(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket
		ORDER BY time_bucket`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemDiskIOTime),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		ValueSum  float64   `ch:"value_sum_val"`
		ValueCnt  uint64    `ch:"value_cnt"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]resourceBucketDTO, len(raw))
	for i, row := range raw {
		var valPtr *float64
		if row.ValueCnt > 0 {
			v := row.ValueSum
			valPtr = &v
		}
		rows[i] = ResourceBucket{
			Timestamp: row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			Pod:       "",
			Value:     valPtr,
		}
	}
	return rows, nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]mountpointBucketDTO, error) {
	b := bucket(startMs, endMs)
	mp := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrFilesystemMountpoint)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as mountpoint, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, mp, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemFilesystemUsage)
	var rows []mountpointBucketDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetFilesystemUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       sumMerge(value_avg_num)  AS value_num,
		       sumMerge(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket
		ORDER BY time_bucket`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemFilesystemUtil),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		ValueNum  float64   `ch:"value_num"`
		ValueCnt  uint64    `ch:"value_cnt"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]resourceBucketDTO, len(raw))
	for i, row := range raw {
		var valPtr *float64
		if row.ValueCnt > 0 {
			v := row.ValueNum / float64(row.ValueCnt)
			valPtr = &v
		}
		rows[i] = ResourceBucket{
			Timestamp: row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			Pod:       "",
			Value:     valPtr,
		}
	}
	return rows, nil
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

type diskMetricRow struct {
	SystemDisk *float64 `ch:"system_disk"`
	RatioDisk  *float64 `ch:"ratio_disk"`
	AttrDisk   *float64 `ch:"attr_disk"`
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
	aDisk := infraconsts.AttrFloat(infraconsts.AttrSystemDiskUtilization)
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
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskUtilization, infraconsts.MetricDiskFree, infraconsts.MetricDiskTotal,
		aDisk)

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

func (r *ClickHouseRepository) queryDiskMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aDisk := infraconsts.AttrFloat(infraconsts.AttrSystemDiskUtilization)
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
		infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemDiskUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskTotal, infraconsts.ColValue,
		fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskFree, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskTotal, infraconsts.ColValue,
		aDisk, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), aDisk, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), aDisk, aDisk,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskUtilization, infraconsts.MetricDiskFree, infraconsts.MetricDiskTotal,
		aDisk)

	var row diskMetricRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, serviceParams(teamID, serviceName, startMs, endMs)...).ScanStruct(&row)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemDisk, row.RatioDisk, row.AttrDisk)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryDiskMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aDisk := infraconsts.AttrFloat(infraconsts.AttrSystemDiskUtilization)
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
		infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemDiskUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskTotal, infraconsts.ColValue,
		fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskFree, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskTotal, infraconsts.ColValue,
		aDisk, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), aDisk, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), aDisk, aDisk,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, colHostCoalesce, colPodCoalesce, colContainerCoalesce, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskUtilization, infraconsts.MetricDiskFree, infraconsts.MetricDiskTotal,
		aDisk)

	var row diskMetricRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...).ScanStruct(&row)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemDisk, row.RatioDisk, row.AttrDisk)
	return calculateAverage(values), nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetAvgDisk(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, svc := range services {
		diskVal, err := r.queryDiskMetricByService(ctx, teamID, svc, startMs, endMs)
		if err == nil && diskVal != nil && *diskVal >= 0 {
			values = append(values, *diskVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetDiskByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryDiskMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetDiskByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryDiskMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
