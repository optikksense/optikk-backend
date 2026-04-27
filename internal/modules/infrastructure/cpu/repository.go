package cpu

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

const metricsGaugesRollupPrefix = "observability.metrics"

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
	GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (loadAverageResultDTO, error)
	GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetCPUByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuServiceMetricDTO, error)
	GetCPUByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuInstanceMetricDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, args []any) ([]stateBucketDTO, error) {
	var rows []stateBucketDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.queryStateBuckets", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       state_dim                AS state,
		       sum(value_sum)      AS value_sum_val,
		       sum(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND state_dim != ''
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemCPUTime),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}
	var raw []struct {
		Timestamp	time.Time	`ch:"time_bucket"`
		State		string		`ch:"state"`
		ValueSum	float64		`ch:"value_sum_val"`
		ValueCnt	uint64		`ch:"value_cnt"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.GetCPUTime", &raw, query, args...); err != nil {
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

// GetCPUUsagePercentage combines system-level CPU utilization + process-level
// CPU usage. Pulls per-metric per-bucket avg from the v2 rollup, folds in Go.
// The raw query's `attributes.'system.cpu.utilization'` fallback path is
// dropped — attribute-based fallback isn't keyed in any rollup.
func (r *ClickHouseRepository) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT
		    toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		    service                                                      AS pod,
		    metric_name                                                  AS metric_name,
		    sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		  AND service != ''
		GROUP BY time_bucket, pod, metric_name
		ORDER BY time_bucket, pod
	`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("metricNames", []string{
			infraconsts.MetricSystemCPUUtilization,
			infraconsts.MetricSystemCPUUsage,
			infraconsts.MetricProcessCPUUsage,
		}),
	}
	var metricRows []struct {
		Timestamp	time.Time	`ch:"time_bucket"`
		Pod		string		`ch:"pod"`
		MetricName	string		`ch:"metric_name"`
		Val		float64		`ch:"val"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.GetCPUUsagePercentage", &metricRows, query, args...); err != nil {
		return nil, err
	}
	// Fold per (bucket, pod): apply the <=1.0 → *100 conversion per row, then
	// average the non-nil per-metric values to get a single CPU percentage.
	type key struct {
		t	time.Time
		pod	string
	}
	folded := map[key][]float64{}
	for _, mr := range metricRows {
		v := mr.Val
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > infraconsts.PercentageThreshold*100 {
			continue
		}
		if v <= infraconsts.PercentageThreshold {
			v = v * infraconsts.PercentageMultiplier
		}
		k := key{mr.Timestamp, mr.Pod}
		folded[k] = append(folded[k], v)
	}
	rows := make([]resourceBucketDTO, 0, len(folded))
	for k, vals := range folded {
		avg := calculateAverage(vals)
		rows = append(rows, ResourceBucket{
			Timestamp:	k.t.UTC().Format("2006-01-02 15:04:05"),
			Pod:		k.pod,
			Value:		avg,
		})
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (loadAverageResultDTO, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)  AS val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", []string{
			infraconsts.MetricSystemCPULoadAvg1m,
			infraconsts.MetricSystemCPULoadAvg5m,
			infraconsts.MetricSystemCPULoadAvg15m,
		}),
	}
	var metricRows []struct {
		MetricName	string	`ch:"metric_name"`
		Val		float64	`ch:"val"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.GetLoadAverage", &metricRows, query, args...); err != nil {
		return loadAverageResultDTO{}, err
	}
	var result loadAverageResultDTO
	for _, mr := range metricRows {
		if math.IsNaN(mr.Val) || math.IsInf(mr.Val, 0) {
			continue
		}
		switch mr.MetricName {
		case infraconsts.MetricSystemCPULoadAvg1m:
			result.Load1m = mr.Val
		case infraconsts.MetricSystemCPULoadAvg5m:
			result.Load5m = mr.Val
		case infraconsts.MetricSystemCPULoadAvg15m:
			result.Load15m = mr.Val
		}
	}
	return result, nil
}

func (r *ClickHouseRepository) GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       state_dim                                                    AS state,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS metric_val
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemProcessCount),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}
	return r.queryStateBuckets(ctx, query, args)
}

type serviceNameRow struct {
	ServiceName string `ch:"service_name"`
}

type instanceRow struct {
	Host		string	`ch:"host"`
	Pod		string	`ch:"pod"`
	Container	string	`ch:"container"`
	ServiceName	string	`ch:"service_name"`
}

// cpuMetricNames drives service/instance DISTINCT + fold queries.
var cpuMetricNames = []string{
	infraconsts.MetricSystemCPUUtilization,
	infraconsts.MetricSystemCPUUsage,
	infraconsts.MetricProcessCPUUsage,
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

// cpuFoldMetricRows normalizes rollup-per-metric rows into a single pct.
func cpuFoldMetricRows(rows []metricValueRow) *float64 {
	byMetric := map[string]float64{}
	for _, r := range rows {
		byMetric[r.MetricName] = r.ValAvg
	}
	var values []float64
	add := func(v float64) {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > infraconsts.PercentageThreshold*100 {
			return
		}
		if v <= infraconsts.PercentageThreshold {
			v = v * infraconsts.PercentageMultiplier
		}
		values = append(values, v)
	}
	if v, ok := byMetric[infraconsts.MetricSystemCPUUtilization]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricSystemCPUUsage]; ok {
		add(v)
	}
	if v, ok := byMetric[infraconsts.MetricProcessCPUUsage]; ok {
		add(v)
	}
	return calculateAverage(values)
}

type metricValueRow struct {
	MetricName	string	`ch:"metric_name"`
	ValAvg		float64	`ch:"val_avg"`
}

func (r *ClickHouseRepository) queryCPUMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)  AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", cpuMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.queryCPUMetricByService", &rows, query, args...); err != nil {
		return nil, err
	}
	return cpuFoldMetricRows(rows), nil
}

func (r *ClickHouseRepository) queryCPUMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container	// container not keyed on metrics_gauges_rollup; see connpool notes.
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT metric_name                                                             AS metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0)  AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND host = @host
		  AND pod = @pod
		  AND service = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", cpuMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.queryCPUMetricByInstance", &rows, query, args...); err != nil {
		return nil, err
	}
	return cpuFoldMetricRows(rows), nil
}

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT DISTINCT service AS service_name
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service != ''
		  AND metric_name IN @metricNames
		ORDER BY service_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", cpuMetricNames),
	}
	var rows []serviceNameRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.getServiceList", &rows, query, args...); err != nil {
		return nil, err
	}
	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = row.ServiceName
	}
	return services, nil
}

func (r *ClickHouseRepository) getInstanceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]instanceRow, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT DISTINCT host AS host, pod AS pod, '' AS container, service AS service_name
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service != ''
		  AND metric_name IN @metricNames
		LIMIT 200`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", cpuMetricNames),
	}
	var rows []instanceRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.getInstanceList", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", cpuMetricNames),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.GetAvgCPU", &rows, query, args...); err != nil {
		return MetricValue{Value: 0}, err
	}
	avg := cpuFoldMetricRows(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetCPUByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuServiceMetricDTO, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT service AS service_name,
		       metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND service != ''
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY service_name, metric_name
		ORDER BY service_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", cpuMetricNames),
	}
	type serviceMetricRow struct {
		ServiceName	string	`ch:"service_name"`
		MetricName	string	`ch:"metric_name"`
		ValAvg		float64	`ch:"val_avg"`
	}
	var rows []serviceMetricRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.GetCPUByService", &rows, query, args...); err != nil {
		return nil, err
	}
	byService := map[string][]metricValueRow{}
	order := []string{}
	for _, row := range rows {
		if _, ok := byService[row.ServiceName]; !ok {
			order = append(order, row.ServiceName)
		}
		byService[row.ServiceName] = append(byService[row.ServiceName], metricValueRow{
			MetricName:	row.MetricName,
			ValAvg:		row.ValAvg,
		})
	}
	result := make([]cpuServiceMetricDTO, 0, len(order))
	for _, name := range order {
		result = append(result, cpuServiceMetricDTO{
			ServiceName:	name,
			Value:		cpuFoldMetricRows(byService[name]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetCPUByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuInstanceMetricDTO, error) {
	table := "observability.signoz_index_v3"
	query := fmt.Sprintf(`
		SELECT host, pod, service AS service_name,
		       metric_name,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND service != ''
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY host, pod, service_name, metric_name
		ORDER BY host, pod, service_name
		LIMIT 1000`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", cpuMetricNames),
	}
	type instanceMetricRow struct {
		Host		string	`ch:"host"`
		Pod		string	`ch:"pod"`
		ServiceName	string	`ch:"service_name"`
		MetricName	string	`ch:"metric_name"`
		ValAvg		float64	`ch:"val_avg"`
	}
	var rows []instanceMetricRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "cpu.GetCPUByInstance", &rows, query, args...); err != nil {
		return nil, err
	}
	type instKey struct{ Host, Pod, Service string }
	byInst := map[instKey][]metricValueRow{}
	order := []instKey{}
	for _, row := range rows {
		k := instKey{row.Host, row.Pod, row.ServiceName}
		if _, ok := byInst[k]; !ok {
			order = append(order, k)
		}
		byInst[k] = append(byInst[k], metricValueRow{
			MetricName:	row.MetricName,
			ValAvg:		row.ValAvg,
		})
	}
	result := make([]cpuInstanceMetricDTO, 0, len(order))
	for _, k := range order {
		result = append(result, cpuInstanceMetricDTO{
			Host:		k.Host,
			Pod:		k.Pod,
			ServiceName:	k.Service,
			Value:		cpuFoldMetricRows(byInst[k]),
		})
	}
	return result, nil
}

// Silence unused-import / unused-helper lint for `nullableToSlice` (still used
// by sibling modules; kept here to match the established per-repo file layout).
var _ = nullableToSlice
