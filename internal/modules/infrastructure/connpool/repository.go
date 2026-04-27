package connpool

import (
	"context"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

const tableMetrics = "observability.metrics"

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

var connpoolMetrics = []string{
	infraconsts.MetricDBConnectionPoolUtilization,
	infraconsts.MetricHikariCPConnectionsActive,
	infraconsts.MetricHikariCPConnectionsMax,
	infraconsts.MetricJDBCConnectionsActive,
	infraconsts.MetricJDBCConnectionsMax,
}

type metricValueRow struct {
	MetricName string  `ch:"metric_name"`
	ValAvg     float64 `ch:"val_avg"`
}

func foldConnPoolMetrics(rows []metricValueRow) *float64 {
	by := make(map[string]float64, len(rows))
	for _, r := range rows {
		by[r.MetricName] = r.ValAvg
	}
	var values []float64
	if v, ok := by[infraconsts.MetricDBConnectionPoolUtilization]; ok {
		if v <= infraconsts.PercentageThreshold {
			values = append(values, v*infraconsts.PercentageMultiplier)
		} else {
			values = append(values, v)
		}
	}
	if max := by[infraconsts.MetricHikariCPConnectionsMax]; max > 0 {
		active := by[infraconsts.MetricHikariCPConnectionsActive]
		values = append(values, infraconsts.PercentageMultiplier*active/max)
	}
	if max := by[infraconsts.MetricJDBCConnectionsMax]; max > 0 {
		active := by[infraconsts.MetricJDBCConnectionsActive]
		values = append(values, infraconsts.PercentageMultiplier*active/max)
	}
	return calculateAverage(values)
}

// connpoolBaseParams builds the params shared by every connpool query: tenant,
// hour-aligned bucket bounds for PREWHERE granule prune, exact ms range for
// sub-bucket precision in WHERE, and the metric_name allow-list.
func connpoolBaseParams(teamID int64, startMs, endMs int64) []any {
	bucketStart := timebucket.MetricsHourBucket(startMs / 1000)
	bucketEnd := timebucket.MetricsHourBucket(endMs / 1000).Add(time.Hour)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", connpoolMetrics),
	}
}

func (r *ClickHouseRepository) GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	query := `
		SELECT metric_name, avg(value) AS val_avg
		FROM ` + tableMetrics + `
		PREWHERE team_id = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY metric_name`
	var rows []metricValueRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.GetAvgConnPool", &rows, query, connpoolBaseParams(teamID, startMs, endMs)...); err != nil {
		return MetricValue{Value: 0}, err
	}
	avg := foldConnPoolMetrics(rows)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetConnPoolByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolServiceMetricDTO, error) {
	query := `
		SELECT service, metric_name, avg(value) AS val_avg
		FROM ` + tableMetrics + `
		PREWHERE team_id = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		WHERE service != ''
		  AND metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY service, metric_name
		ORDER BY service`
	type serviceMetricRow struct {
		ServiceName string  `ch:"service"`
		MetricName  string  `ch:"metric_name"`
		ValAvg      float64 `ch:"val_avg"`
	}
	var rows []serviceMetricRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.GetConnPoolByService", &rows, query, connpoolBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	byService := map[string][]metricValueRow{}
	order := []string{}
	for _, row := range rows {
		if _, ok := byService[row.ServiceName]; !ok {
			order = append(order, row.ServiceName)
		}
		byService[row.ServiceName] = append(byService[row.ServiceName], metricValueRow{
			MetricName: row.MetricName,
			ValAvg:     row.ValAvg,
		})
	}
	result := make([]connPoolServiceMetricDTO, 0, len(order))
	for _, name := range order {
		result = append(result, connPoolServiceMetricDTO{
			ServiceName: name,
			Value:       foldConnPoolMetrics(byService[name]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetConnPoolByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolInstanceMetricDTO, error) {
	// pod is read from resource.`k8s.pod.name` JSON typed-path — there is no
	// top-level pod column on observability.metrics today.
	query := `
		SELECT host,
		       resource.` + "`k8s.pod.name`" + `::String AS pod,
		       service,
		       metric_name,
		       avg(value) AS val_avg
		FROM ` + tableMetrics + `
		PREWHERE team_id = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		WHERE service != ''
		  AND metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		GROUP BY host, pod, service, metric_name
		ORDER BY host, pod, service
		LIMIT 1000`
	type instanceMetricRow struct {
		Host        string  `ch:"host"`
		Pod         string  `ch:"pod"`
		ServiceName string  `ch:"service"`
		MetricName  string  `ch:"metric_name"`
		ValAvg      float64 `ch:"val_avg"`
	}
	var rows []instanceMetricRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "connpool.GetConnPoolByInstance", &rows, query, connpoolBaseParams(teamID, startMs, endMs)...); err != nil {
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
			MetricName: row.MetricName,
			ValAvg:     row.ValAvg,
		})
	}
	result := make([]connPoolInstanceMetricDTO, 0, len(order))
	for _, k := range order {
		result = append(result, connPoolInstanceMetricDTO{
			Host:        k.Host,
			Pod:         k.Pod,
			ServiceName: k.Service,
			Value:       foldConnPoolMetrics(byInst[k]),
		})
	}
	return result, nil
}
