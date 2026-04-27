package network

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
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
	GetNetworkIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error)
	GetNetworkPackets(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error)
	GetNetworkErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetNetworkDropped(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetNetworkConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetNetworkByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error)
	GetNetworkByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error)
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
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.queryDirectionBuckets", &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	var rows []stateBucketDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.queryStateBuckets", &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) queryResourceBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	var rows []resourceBucketDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.queryResourceBuckets", &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) queryDirectionBucketsFromRollup(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]directionBucketDTO, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS time_bucket,
		       state_dim                AS direction,
		       sum(value_sum)      AS value_sum_val,
		       sum(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND state_dim != ''
		GROUP BY time_bucket, direction
		ORDER BY time_bucket, direction`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp	time.Time	`ch:"time_bucket"`
		Direction	string		`ch:"direction"`
		ValueSum	float64		`ch:"value_sum_val"`
		ValueCnt	uint64		`ch:"value_cnt"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.queryDirectionBucketsFromRollup", &raw, query, args...); err != nil {
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
			Timestamp:	row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			Direction:	row.Direction,
			Value:		valPtr,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetNetworkIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBucketsFromRollup(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkIO)
}

func (r *ClickHouseRepository) GetNetworkPackets(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBucketsFromRollup(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkPackets)
}

func (r *ClickHouseRepository) GetNetworkErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS time_bucket,
		       state_dim                AS state,
		       sum(value_sum)      AS value_sum_val,
		       sum(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND state_dim != ''
		GROUP BY time_bucket, state
		ORDER BY time_bucket, state`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkErrors),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp	time.Time	`ch:"time_bucket"`
		State		string		`ch:"state"`
		ValueSum	float64		`ch:"value_sum_val"`
		ValueCnt	uint64		`ch:"value_cnt"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.GetNetworkErrors", &raw, query, args...); err != nil {
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

func (r *ClickHouseRepository) GetNetworkDropped(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	table := "observability.spans"
	tierStep := int64(1)
	query := fmt.Sprintf(`
		SELECT ts_bucket AS time_bucket,
		       sum(value_sum)      AS value_sum_val,
		       sum(sample_count)   AS value_cnt
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		GROUP BY time_bucket
		ORDER BY time_bucket`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // domain-bounded
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkDropped),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp	time.Time	`ch:"time_bucket"`
		ValueSum	float64		`ch:"value_sum_val"`
		ValueCnt	uint64		`ch:"value_cnt"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.GetNetworkDropped", &raw, query, args...); err != nil {
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
			Timestamp:	row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			Pod:		"",
			Value:		valPtr,
		}
	}
	return rows, nil
}

// GetNetworkConnections reads per-network-state avg from metrics_gauges_rollup.
// `state_dim` uses system.network.state with fallback to direction for connections.
func (r *ClickHouseRepository) GetNetworkConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
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
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkConnections),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}
	var rows []stateBucketDTO
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.GetNetworkConnections", &rows, query, args...)
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

type netMetricRow struct {
	SystemNet	*float64	`ch:"system_net"`
	AttrNet		*float64	`ch:"attr_net"`
}

func serviceParams(teamID int64, serviceName string, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func instanceParams(teamID int64, host, pod, container, serviceName string, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
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

type netMetricValueRow struct {
	ValAvg float64 `ch:"val_avg"`
}

// netFoldValue normalizes the single metric (system.network.utilization)
// via the <=1.0 → *100 logic.
func netFoldValue(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return nil
	}
	if v <= infraconsts.PercentageThreshold {
		v = v * infraconsts.PercentageMultiplier
	}
	return &v
}

func (r *ClickHouseRepository) queryNetworkMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkUtilization),
	}
	var row netMetricValueRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return nil, err
	}
	return netFoldValue(row.ValAvg), nil
}

func (r *ClickHouseRepository) queryNetworkMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	_ = container
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND host = @host
		  AND pod = @pod
		  AND service = @serviceName
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkUtilization),
	}
	var row netMetricValueRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return nil, err
	}
	return netFoldValue(row.ValAvg), nil
}

// GetAvgNetwork returns the mean of per-service network utilization across
// every service emitting system.network.utilization in the window. One CH
// query (`GROUP BY service`) replaces the former N+1 pattern (listServices
// then per-service lookup). netFoldValue is applied per service before
// averaging so the <=1.0 -> *100 normalization is preserved.
func (r *ClickHouseRepository) GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	table := "observability.spans"
	query := fmt.Sprintf(`
		SELECT service,
		       sum(value_avg_num) / nullIf(toFloat64(sum(sample_count)), 0) AS val_avg
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND service != ''
		  AND metric_name = @metricName
		GROUP BY service
		HAVING val_avg IS NOT NULL`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkUtilization),
	}

	var rows []struct {
		Service	string	`ch:"service"`
		ValAvg	float64	`ch:"val_avg"`
	}
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "network.GetAvgNetwork", &rows, query, args...); err != nil {
		return MetricValue{Value: 0}, err
	}

	values := make([]float64, 0, len(rows))
	for _, row := range rows {
		if netVal := netFoldValue(row.ValAvg); netVal != nil && *netVal >= 0 {
			values = append(values, *netVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetNetworkByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryNetworkMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetNetworkByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryNetworkMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
