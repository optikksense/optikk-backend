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

const (
	metricsGaugesRollupPrefix   = "observability.metrics_gauges_rollup"
	metricsGaugesV2RollupPrefix = "observability.metrics_gauges_rollup_v2"
)

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

// GetFilesystemUsage reads per-mountpoint avg from metrics_gauges_rollup_v2,
// which extracts `system.filesystem.mountpoint` into `state_dim` for the
// `system.filesystem.usage` / `system.filesystem.utilization` metric family.
func (r *ClickHouseRepository) GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]mountpointBucketDTO, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesV2RollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       state_dim                                                    AS mountpoint,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0) AS metric_val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket, mountpoint
		ORDER BY time_bucket, mountpoint`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemFilesystemUsage),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}
	var rows []mountpointBucketDTO
	return rows, r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...)
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

var diskMetricNames = []string{
	infraconsts.MetricSystemDiskUtilization,
	infraconsts.MetricDiskFree,
	infraconsts.MetricDiskTotal,
}

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	table, _ := rollup.TierTableFor(metricsGaugesV2RollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT DISTINCT service AS service_name
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service != ''
		  AND metric_name IN @metricNames
		ORDER BY service_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", diskMetricNames),
	}
	var rows []serviceNameRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = row.ServiceName
	}
	return services, nil
}

type diskMetricValueRow struct {
	MetricName string  `ch:"metric_name"`
	ValAvg     float64 `ch:"val_avg"`
	ValSum     float64 `ch:"val_sum"`
}

// diskFoldMetricRows converts per-metric rows into a single disk-utilization
// percentage. Same logic as the prior raw query:
//   - MetricSystemDiskUtilization: 0-1 ratio or 0-100 pct; normalize to pct.
//   - MetricDiskFree/MetricDiskTotal: used% = 100 * (1 - free/total).
func diskFoldMetricRows(rows []diskMetricValueRow) *float64 {
	by := map[string]float64{}
	bySum := map[string]float64{}
	for _, r := range rows {
		by[r.MetricName] = r.ValAvg
		bySum[r.MetricName] = r.ValSum
	}
	var values []float64
	if v, ok := by[infraconsts.MetricSystemDiskUtilization]; ok {
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
			if v <= infraconsts.PercentageThreshold {
				v = v * infraconsts.PercentageMultiplier
			}
			values = append(values, v)
		}
	}
	if total := bySum[infraconsts.MetricDiskTotal]; total > 0 {
		free := bySum[infraconsts.MetricDiskFree]
		used := infraconsts.PercentageMultiplier * (1.0 - free/total)
		values = append(values, used)
	}
	return calculateAverage(values)
}

func (r *ClickHouseRepository) queryDiskMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	table, _ := rollup.TierTableFor(metricsGaugesV2RollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)  AS val_avg,
		       toFloat64(sumMerge(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", diskMetricNames),
	}
	var rows []diskMetricValueRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return diskFoldMetricRows(rows), nil
}

func (r *ClickHouseRepository) queryDiskMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container
	table, _ := rollup.TierTableFor(metricsGaugesV2RollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)  AS val_avg,
		       toFloat64(sumMerge(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND host = @host
		  AND pod = @pod
		  AND service = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", diskMetricNames),
	}
	var rows []diskMetricValueRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return diskFoldMetricRows(rows), nil
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
