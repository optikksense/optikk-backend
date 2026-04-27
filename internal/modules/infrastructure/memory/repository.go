package memory

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

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

var memMetricNames = []string{
	infraconsts.MetricSystemMemoryUtilization,
	infraconsts.MetricSystemMemoryUsage,
	infraconsts.MetricJVMMemoryUsed,
	infraconsts.MetricJVMMemoryMax,
}

type metricValueRow struct {
	MetricName	string	`ch:"metric_name"`
	ValAvg		float64	`ch:"val_avg"`
	ValSum		float64	`ch:"val_sum"`
}

// memFoldMetricRows converts per-metric-name rollup results into a single
// memory-usage-percent via the same logic as the prior raw query.
func memFoldMetricRows(rows []metricValueRow) *float64 {
	by := make(map[string]float64, len(rows))
	bySum := make(map[string]float64, len(rows))
	for _, r := range rows {
		by[r.MetricName] = r.ValAvg
		bySum[r.MetricName] = r.ValSum
	}
	var values []float64
	if v, ok := by[infraconsts.MetricSystemMemoryUtilization]; ok {
		if !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
			if v <= infraconsts.PercentageThreshold {
				v = v * infraconsts.PercentageMultiplier
			}
			values = append(values, v)
		}
	}
	if max := bySum[infraconsts.MetricJVMMemoryMax]; max > 0 {
		used := bySum[infraconsts.MetricJVMMemoryUsed]
		values = append(values, infraconsts.PercentageMultiplier*used/max)
	}
	return calculateAverage(values)
}

func (r *ClickHouseRepository) GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS time_bucket,
		       state_dim                                                    AS state,
		       sum(value_sum)                                          AS value_sum_val,
		       sum(sample_count)                                       AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND state_dim != ''
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemMemoryUsage),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}
	var raw []struct {
		Timestamp	time.Time	`ch:"time_bucket"`
		State		string		`ch:"state"`
		ValueSum	float64		`ch:"value_sum_val"`
		ValueCnt	uint64		`ch:"value_cnt"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.GetMemoryUsage", &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]stateBucketDTO, len(raw))
	for i, row := range raw {
		var valPtr *float64
		if row.ValueCnt > 0 {
			v := row.ValueSum
			valPtr = &v
		}
		rows[i] = StateBucket{
			Timestamp:	row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			State:		row.State,
			Value:		valPtr,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT
		    ts_bucket AS time_bucket,
		    service                                                      AS pod,
		    metric_name                                                  AS metric_name,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg,
		    toFloat64(sum(value_sum))                               AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		  AND service != ''
		GROUP BY time_bucket, pod, metric_name
		ORDER BY time_bucket, pod`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("metricNames", memMetricNames),
	}
	var metricRows []struct {
		Timestamp	time.Time	`ch:"time_bucket"`
		Pod		string		`ch:"pod"`
		MetricName	string		`ch:"metric_name"`
		ValAvg		float64		`ch:"val_avg"`
		ValSum		float64		`ch:"val_sum"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.GetMemoryUsagePercentage", &metricRows, query, args...); err != nil {
		return nil, err
	}
	type key struct {
		t	time.Time
		pod	string
	}
	folded := map[key][]metricValueRow{}
	for _, mr := range metricRows {
		k := key{mr.Timestamp, mr.Pod}
		folded[k] = append(folded[k], metricValueRow{MetricName: mr.MetricName, ValAvg: mr.ValAvg, ValSum: mr.ValSum})
	}
	rows := make([]resourceBucketDTO, 0, len(folded))
	for k, group := range folded {
		avg := memFoldMetricRows(group)
		rows = append(rows, ResourceBucket{
			Timestamp:	k.t.UTC().Format("2006-01-02 15:04:05"),
			Pod:		k.pod,
			Value:		avg,
		})
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS time_bucket,
		       state_dim                                                    AS state,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS metric_val
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemPagingUsage),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}
	var rows []stateBucketDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.GetSwapUsage", &rows, query, args...)
}

func (r *ClickHouseRepository) queryMemoryMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)  AS val_avg,
		       toFloat64(sum(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", memMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.queryMemoryMetricByService", &rows, query, args...); err != nil {
		return nil, err
	}
	return memFoldMetricRows(rows), nil
}

func (r *ClickHouseRepository) queryMemoryMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)  AS val_avg,
		       toFloat64(sum(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND host = @host
		  AND pod = @pod
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", memMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.queryMemoryMetricByInstance", &rows, query, args...); err != nil {
		return nil, err
	}
	return memFoldMetricRows(rows), nil
}

type serviceNameRow struct {
	ServiceName string `ch:"service"`
}

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT DISTINCT service AS service
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND service != ''
		  AND metric_name IN @metricNames
		ORDER BY service`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", memMetricNames),
	}
	var rows []serviceNameRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.getServiceList", &rows, query, args...); err != nil {
		return nil, err
	}
	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = row.ServiceName
	}
	return services, nil
}

func (r *ClickHouseRepository) GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg,
		       toFloat64(sum(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", memMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.GetAvgMemory", &rows, query, args...); err != nil {
		return MetricValue{Value: 0}, err
	}
	avg := memFoldMetricRows(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)  AS val_avg,
		       toFloat64(sum(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", memMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.GetMemoryByService", &rows, query, args...); err != nil {
		return nil, err
	}
	return memFoldMetricRows(rows), nil
}

func (r *ClickHouseRepository) GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)  AS val_avg,
		       toFloat64(sum(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND host = @host
		  AND pod = @pod
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", memMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "memory.GetMemoryByInstance", &rows, query, args...); err != nil {
		return nil, err
	}
	return memFoldMetricRows(rows), nil
}
