package connpool

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

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

// DTO for queries returning multiple computed metric columns.
type connMetricRow struct {
	SystemConn *float64 `ch:"system_conn"`
	HikariConn *float64 `ch:"hikari_conn"`
	JDBCConn   *float64 `ch:"jdbc_conn"`
	AttrConn   *float64 `ch:"attr_conn"`
}

type serviceNameRow struct {
	ServiceName string `ch:"service_name"`
}

type instanceRow struct {
	Host        string `ch:"host"`
	Pod         string `ch:"pod"`
	Container   string `ch:"container"`
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

// nullableToSlice converts nullable float64 pointers to a []float64 of non-nil values.
func nullableToSlice(ptrs ...*float64) []float64 {
	out := make([]float64, 0, len(ptrs))
	for _, p := range ptrs {
		if p != nil && !math.IsNaN(*p) && !math.IsInf(*p, 0) {
			out = append(out, *p)
		}
	}
	return out
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

func (r *ClickHouseRepository) queryConnPoolMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	aConn := infraconsts.AttrFloat(infraconsts.AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %.1f, %s * %.1f, %s), %s = '%s' AND isFinite(%s)) as system_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %.1f * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as hikari_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %.1f * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jdbc_conn,
			avgIf(if(%s <= %.1f, %s * %.1f, %s), %s > 0) as attr_conn
		FROM %s
		WHERE %s = @teamID AND %s = @serviceName AND %s BETWEEN @start AND @end
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR %s > 0
		  )`,
		infraconsts.ColValue, infraconsts.PercentageThreshold, infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDBConnectionPoolUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsMax, infraconsts.ColValue,
		infraconsts.PercentageMultiplier, infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsActive, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsMax, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsMax, infraconsts.ColValue,
		infraconsts.PercentageMultiplier, infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsActive, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsMax, infraconsts.ColValue,
		aConn, infraconsts.PercentageThreshold, aConn, infraconsts.PercentageMultiplier, aConn, aConn,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricDBConnectionPoolUtilization, infraconsts.MetricHikariCPConnectionsActive, infraconsts.MetricHikariCPConnectionsMax, infraconsts.MetricJDBCConnectionsActive, infraconsts.MetricJDBCConnectionsMax,
		aConn)

	var row connMetricRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, serviceParams(teamID, serviceName, startMs, endMs)...).ScanStruct(&row)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemConn, row.HikariConn, row.JDBCConn, row.AttrConn)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) queryConnPoolMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	aConn := infraconsts.AttrFloat(infraconsts.AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT
			avgIf(if(%s <= %.1f, %s * %.1f, %s), %s = '%s' AND isFinite(%s)) as system_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %.1f * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as hikari_conn,
			if(sumIf(%s, %s = '%s' AND %s > 0) > 0,
			   %.1f * sumIf(%s, %s = '%s' AND %s >= 0) / nullIf(sumIf(%s, %s = '%s' AND %s > 0), 0),
			   NULL) as jdbc_conn,
			avgIf(if(%s <= %.1f, %s * %.1f, %s), %s > 0) as attr_conn
		FROM %s
		WHERE %s = @teamID AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName AND %s BETWEEN @start AND @end
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR %s > 0
		  )`,
		infraconsts.ColValue, infraconsts.PercentageThreshold, infraconsts.ColValue, infraconsts.PercentageMultiplier, infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDBConnectionPoolUtilization, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsMax, infraconsts.ColValue,
		infraconsts.PercentageMultiplier, infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsActive, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsMax, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsMax, infraconsts.ColValue,
		infraconsts.PercentageMultiplier, infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsActive, infraconsts.ColValue,
		infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsMax, infraconsts.ColValue,
		aConn, infraconsts.PercentageThreshold, aConn, infraconsts.PercentageMultiplier, aConn, aConn,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColHost, infraconsts.ColPod, infraconsts.ColContainer, infraconsts.ColServiceName, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricDBConnectionPoolUtilization, infraconsts.MetricHikariCPConnectionsActive, infraconsts.MetricHikariCPConnectionsMax, infraconsts.MetricJDBCConnectionsActive, infraconsts.MetricJDBCConnectionsMax,
		aConn)

	var row connMetricRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs)...).ScanStruct(&row)
	if err != nil {
		return nil, err
	}

	values := nullableToSlice(row.SystemConn, row.HikariConn, row.JDBCConn, row.AttrConn)
	return calculateAverage(values), nil
}

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	aConn := infraconsts.AttrFloat(infraconsts.AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s != ''
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR %s > 0
		  )
		ORDER BY service_name`,
		infraconsts.ColServiceName,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColServiceName,
		infraconsts.ColMetricName, infraconsts.MetricDBConnectionPoolUtilization, infraconsts.MetricHikariCPConnectionsActive, infraconsts.MetricHikariCPConnectionsMax, infraconsts.MetricJDBCConnectionsActive, infraconsts.MetricJDBCConnectionsMax,
		aConn)

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

func (r *ClickHouseRepository) getInstanceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]instanceRow, error) {
	aConn := infraconsts.AttrFloat(infraconsts.AttrDBConnectionPoolUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as host, %s as pod, %s as container, %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s != ''
		  AND (
		      %s IN ('%s', '%s', '%s', '%s', '%s')
		      OR %s > 0
		  )
		LIMIT 200`,
		infraconsts.ColHost, infraconsts.ColPod, infraconsts.ColContainer, infraconsts.ColServiceName,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColServiceName,
		infraconsts.ColMetricName, infraconsts.MetricDBConnectionPoolUtilization, infraconsts.MetricHikariCPConnectionsActive, infraconsts.MetricHikariCPConnectionsMax, infraconsts.MetricJDBCConnectionsActive, infraconsts.MetricJDBCConnectionsMax,
		aConn)

	var rows []instanceRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetAvgConnPool(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, service := range services {
		connVal, err := r.queryConnPoolMetricByService(ctx, teamID, service, startMs, endMs)
		if err == nil && connVal != nil && *connVal >= 0 {
			values = append(values, *connVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetConnPoolByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolServiceMetricDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	result := make([]connPoolServiceMetricDTO, len(services))
	for i, serviceName := range services {
		connVal, _ := r.queryConnPoolMetricByService(ctx, teamID, serviceName, startMs, endMs)
		result[i] = connPoolServiceMetricDTO{
			ServiceName: serviceName,
			Value:       connVal,
		}
	}
	return result, nil
}

func (r *ClickHouseRepository) GetConnPoolByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]connPoolInstanceMetricDTO, error) {
	instances, err := r.getInstanceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	result := make([]connPoolInstanceMetricDTO, len(instances))
	for i, inst := range instances {
		connVal, _ := r.queryConnPoolMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		result[i] = connPoolInstanceMetricDTO{
			Host:        inst.Host,
			Pod:         inst.Pod,
			Container:   inst.Container,
			ServiceName: inst.ServiceName,
			Value:       connVal,
		}
	}
	return result, nil
}
