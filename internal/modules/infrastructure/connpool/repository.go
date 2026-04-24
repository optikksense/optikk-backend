package connpool

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

// metrics_gauges_rollup — extended state_dim (db.client.connections.state, …).
// connpool reads gauge values (Hikari/JDBC active/max + generic
// db.client.connection_pool.utilization) per service/instance and folds
// them client-side. The `db.connection_pool.utilization` attribute-based
// fallback in the prior raw query is dropped — that attribute isn't keyed
// in any rollup. In typical OTel-for-DB setups ingestion emits one of the
// canonical metric_names, so loss is bounded.
const metricsGaugesRollupPrefix = "observability.metrics_gauges_rollup"

type Repository interface {
	GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetConnPoolByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolServiceMetricDTO, error)
	GetConnPoolByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolInstanceMetricDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

type instanceRow struct {
	Host		string	`ch:"host"`
	Pod		string	`ch:"pod"`
	Container	string	`ch:"container"`
	ServiceName	string	`ch:"service_name"`
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

// connpoolMetrics is the metric_name list the rollup query selects on.
var connpoolMetrics = []string{
	infraconsts.MetricDBConnectionPoolUtilization,
	infraconsts.MetricHikariCPConnectionsActive,
	infraconsts.MetricHikariCPConnectionsMax,
	infraconsts.MetricJDBCConnectionsActive,
	infraconsts.MetricJDBCConnectionsMax,
}

// metricValueRow captures per-metric-name avg+sum from the rollup scan.
type metricValueRow struct {
	MetricName	string	`ch:"metric_name"`
	ValAvg		float64	`ch:"val_avg"`
	ValSum		float64	`ch:"val_sum"`
}

// foldConnPoolMetrics converts per-metric-name rollup rows into a single
// pool-utilization percentage, folded across the available signals. Logic
// mirrors the prior raw query:
//   - MetricDBConnectionPoolUtilization: value is ratio (0-1) or pct (0-100);
//     normalize to pct via `if v <= threshold → v*100 else v`.
//   - HikariCP / JDBC: pct = multiplier * sum(active) / sum(max).
func foldConnPoolMetrics(rows []metricValueRow) *float64 {
	by := make(map[string]float64, len(rows))
	bySum := make(map[string]float64, len(rows))
	for _, r := range rows {
		by[r.MetricName] = r.ValAvg
		bySum[r.MetricName] = r.ValSum
	}
	var values []float64
	if v, ok := by[infraconsts.MetricDBConnectionPoolUtilization]; ok {
		if v <= infraconsts.PercentageThreshold {
			values = append(values, v*infraconsts.PercentageMultiplier)
		} else {
			values = append(values, v)
		}
	}
	if max := bySum[infraconsts.MetricHikariCPConnectionsMax]; max > 0 {
		active := bySum[infraconsts.MetricHikariCPConnectionsActive]
		values = append(values, infraconsts.PercentageMultiplier*active/max)
	}
	if max := bySum[infraconsts.MetricJDBCConnectionsMax]; max > 0 {
		active := bySum[infraconsts.MetricJDBCConnectionsActive]
		values = append(values, infraconsts.PercentageMultiplier*active/max)
	}
	return calculateAverage(values)
}

func (r *ClickHouseRepository) queryConnPoolMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT
		    metric_name                                                                AS metric_name,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)     AS val_avg,
		    toFloat64(sumMerge(value_sum))                                             AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND service = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name
	`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", connpoolMetrics),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.queryConnPoolMetricByService", &rows, query, args...); err != nil {
		return nil, err
	}
	return foldConnPoolMetrics(rows), nil
}

func (r *ClickHouseRepository) queryConnPoolMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	// Rollup has host + pod + service as keys; container isn't a key there
	// (that's only in metrics_k8s_rollup). Filter on host+pod+service; the
	// container arg is ignored. Acceptable because conn-pool utilization is
	// typically scoped to pod granularity in practice.
	_ = container
	query := fmt.Sprintf(`
		SELECT
		    metric_name                                                                AS metric_name,
		    sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)     AS val_avg,
		    toFloat64(sumMerge(value_sum))                                             AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND host = @host
		  AND pod = @pod
		  AND service = @serviceName
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name
	`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec // G115
		clickhouse.Named("host", host),
		clickhouse.Named("pod", pod),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", connpoolMetrics),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.queryConnPoolMetricByInstance", &rows, query, args...); err != nil {
		return nil, err
	}
	return foldConnPoolMetrics(rows), nil
}

type serviceNameRow struct {
	ServiceName string `ch:"service_name"`
}

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT DISTINCT service AS service_name
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service != ''
		  AND metric_name IN @metricNames
		ORDER BY service_name
	`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", connpoolMetrics),
	}
	var rows []serviceNameRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.getServiceList", &rows, query, args...); err != nil {
		return nil, err
	}
	services := make([]string, len(rows))
	for i, row := range rows {
		services[i] = row.ServiceName
	}
	return services, nil
}

func (r *ClickHouseRepository) getInstanceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]instanceRow, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	// container isn't a rollup key (see note on queryConnPoolMetricByInstance);
	// returned as empty string.
	query := fmt.Sprintf(`
		SELECT DISTINCT host AS host, pod AS pod, '' AS container, service AS service_name
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND service != ''
		  AND metric_name IN @metricNames
		LIMIT 200
	`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),	//nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", connpoolMetrics),
	}
	var rows []instanceRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.getInstanceList", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT metric_name,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)  AS val_avg,
		       toFloat64(sumMerge(value_sum))                                          AS val_sum
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name IN @metricNames
		GROUP BY metric_name`, table)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", connpoolMetrics),
	}
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.GetAvgConnPool", &rows, query, args...); err != nil {
		return MetricValue{Value: 0}, err
	}
	avg := foldConnPoolMetrics(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetConnPoolByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolServiceMetricDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service AS service_name,
		       metric_name,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)  AS val_avg,
		       toFloat64(sumMerge(value_sum))                                          AS val_sum
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
		clickhouse.Named("metricNames", connpoolMetrics),
	}
	type serviceMetricRow struct {
		ServiceName	string	`ch:"service_name"`
		MetricName	string	`ch:"metric_name"`
		ValAvg		float64	`ch:"val_avg"`
		ValSum		float64	`ch:"val_sum"`
	}
	var rows []serviceMetricRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.GetConnPoolByService", &rows, query, args...); err != nil {
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
			ValSum:		row.ValSum,
		})
	}
	result := make([]connPoolServiceMetricDTO, 0, len(order))
	for _, name := range order {
		result = append(result, connPoolServiceMetricDTO{
			ServiceName:	name,
			Value:		foldConnPoolMetrics(byService[name]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetConnPoolByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolInstanceMetricDTO, error) {
	table, _ := rollup.TierTableFor(metricsGaugesRollupPrefix, startMs, endMs)
	query := fmt.Sprintf(`
		SELECT host, pod, service AS service_name,
		       metric_name,
		       sumMerge(value_avg_num) / nullIf(toFloat64(sumMerge(sample_count)), 0)  AS val_avg,
		       toFloat64(sumMerge(value_sum))                                          AS val_sum
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
		clickhouse.Named("metricNames", connpoolMetrics),
	}
	type instanceMetricRow struct {
		Host		string	`ch:"host"`
		Pod		string	`ch:"pod"`
		ServiceName	string	`ch:"service_name"`
		MetricName	string	`ch:"metric_name"`
		ValAvg		float64	`ch:"val_avg"`
		ValSum		float64	`ch:"val_sum"`
	}
	var rows []instanceMetricRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.GetConnPoolByInstance", &rows, query, args...); err != nil {
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
			ValSum:		row.ValSum,
		})
	}
	result := make([]connPoolInstanceMetricDTO, 0, len(order))
	for _, k := range order {
		result = append(result, connPoolInstanceMetricDTO{
			Host:		k.Host,
			Pod:		k.Pod,
			ServiceName:	k.Service,
			Value:		foldConnPoolMetrics(byInst[k]),
		})
	}
	return result, nil
}
