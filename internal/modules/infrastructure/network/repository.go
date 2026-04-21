package network

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
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	var rows []stateBucketDTO
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

func (r *ClickHouseRepository) GetNetworkIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBucketsFromRollup(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkIO)
}

func (r *ClickHouseRepository) GetNetworkPackets(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBucketsFromRollup(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkPackets)
}

func (r *ClickHouseRepository) GetNetworkErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	table, tierStep := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       state_dim                AS state,
		       sumMerge(value_sum)      AS value_sum_val,
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
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkErrors),
		clickhouse.Named("intervalMin", queryIntervalMinutes(tierStep, startMs, endMs)),
	}

	var raw []struct {
		Timestamp time.Time `ch:"time_bucket"`
		State     string    `ch:"state"`
		ValueSum  float64   `ch:"value_sum_val"`
		ValueCnt  uint64    `ch:"value_cnt"`
	}
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, args...); err != nil {
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
			Timestamp: row.Timestamp.UTC().Format("2006-01-02 15:04:05"),
			State:     row.State,
			Value:     valPtr,
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetNetworkDropped(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
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
		clickhouse.Named("metricName", infraconsts.MetricSystemNetworkDropped),
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
func (r *ClickHouseRepository) GetNetworkConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemNetworkState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, avg(%s) as metric_val
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemNetworkConnections)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

type netMetricRow struct {
	SystemNet *float64 `ch:"system_net"`
	AttrNet   *float64 `ch:"attr_net"`
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
	aNet := infraconsts.AttrFloat(infraconsts.AttrSystemNetworkUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s != ''
		  AND (
		      %s = '%s'
		      OR %s > 0
		  )
		ORDER BY service_name`,
		infraconsts.ColServiceName,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColServiceName,
		infraconsts.ColMetricName, infraconsts.MetricSystemNetworkUtilization,
		aNet)

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

func (r *ClickHouseRepository) queryNetworkMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aNet := infraconsts.AttrFloat(infraconsts.AttrSystemNetworkUtilization)
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
		infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemNetworkUtilization, infraconsts.ColValue,
		aNet, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), aNet, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), aNet, aNet,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemNetworkUtilization,
		aNet)

	var row netMetricRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, serviceParams(teamID, serviceName, startMs, endMs)...).ScanStruct(&row)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemNet, row.AttrNet)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryNetworkMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aNet := infraconsts.AttrFloat(infraconsts.AttrSystemNetworkUtilization)
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
		infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), infraconsts.ColValue, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricSystemNetworkUtilization, infraconsts.ColValue,
		aNet, fmt.Sprintf("%.1f", infraconsts.PercentageThreshold), aNet, fmt.Sprintf("%.1f", infraconsts.PercentageMultiplier), aNet, aNet,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, colHostCoalesce, colPodCoalesce, colContainerCoalesce, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemNetworkUtilization,
		aNet)

	var row netMetricRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...).ScanStruct(&row)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemNet, row.AttrNet)
	return calculateAverage(values), nil
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetAvgNetwork(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, svc := range services {
		netVal, err := r.queryNetworkMetricByService(ctx, teamID, svc, startMs, endMs)
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

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetNetworkByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryNetworkMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

// TODO(phase8): rollup migration requires multi-metric fallback logic not yet supported by metrics_gauges_rollup
func (r *ClickHouseRepository) GetNetworkByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryNetworkMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
