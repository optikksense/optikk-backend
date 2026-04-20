package connpool

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
	"golang.org/x/sync/errgroup"
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

// rescaleExpr emits a pure arithmetic rescale: if col<=1.0 multiply by 100,
// else leave unchanged. Uses boolean→UInt8 coercion so no if/multiIf is needed.
func rescaleExpr(col string) string {
	return fmt.Sprintf("(%s * (1 + 99 * (%s <= %.1f)))", col, col, infraconsts.PercentageThreshold)
}

type sumCountRow struct {
	Sum   float64 `ch:"s"`
	Count uint64  `ch:"c"`
}

type activeMaxRow struct {
	ActiveSum float64 `ch:"active_sum"`
	MaxSum    float64 `ch:"max_sum"`
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

// queryConnPoolMetric runs the four legs (system utilization, HikariCP ratio,
// JDBC ratio, attribute utilization) as parallel errgroup scans. Each leg is
// a plain filter + (sum, count) aggregate; the mean is composed in Go. No
// sumIf/avgIf/if/nullIf/coalesce in SELECT.
func (r *ClickHouseRepository) queryConnPoolMetric(ctx context.Context, scopeWhere string, args []any) (*float64, error) {
	aConn := infraconsts.AttrFloat(infraconsts.AttrDBConnectionPoolUtilization)

	var (
		sys    sumCountRow
		attr   sumCountRow
		hikari activeMaxRow
		jdbc   activeMaxRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s = '%s'
			  AND isFinite(%s)
			  %s`,
			rescaleExpr(infraconsts.ColValue),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricDBConnectionPoolUtilization,
			infraconsts.ColValue,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&sys)
	})

	g.Go(func() error {
		// Hikari active + max legs: sum each separately so Go can derive
		// util% = 100*active/max.
		query := fmt.Sprintf(`
			SELECT
				sum(%s * (%s = '%s')) as active_sum,
				sum(%s * (%s = '%s')) as max_sum
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s IN ('%s', '%s')
			  AND %s >= 0
			  %s`,
			infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsActive,
			infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsMax,
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricHikariCPConnectionsActive, infraconsts.MetricHikariCPConnectionsMax,
			infraconsts.ColValue,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&hikari)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT
				sum(%s * (%s = '%s')) as active_sum,
				sum(%s * (%s = '%s')) as max_sum
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s IN ('%s', '%s')
			  AND %s >= 0
			  %s`,
			infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsActive,
			infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsMax,
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricJDBCConnectionsActive, infraconsts.MetricJDBCConnectionsMax,
			infraconsts.ColValue,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&jdbc)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s > 0
			  AND isFinite(%s)
			  %s`,
			rescaleExpr(aConn),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			aConn, aConn,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&attr)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var sysPtr, hikariPtr, jdbcPtr, attrPtr *float64
	if sys.Count > 0 {
		v := sys.Sum / float64(sys.Count)
		sysPtr = &v
	}
	if attr.Count > 0 {
		v := attr.Sum / float64(attr.Count)
		attrPtr = &v
	}
	if hikari.MaxSum > 0 {
		v := infraconsts.PercentageMultiplier * hikari.ActiveSum / hikari.MaxSum
		hikariPtr = &v
	}
	if jdbc.MaxSum > 0 {
		v := infraconsts.PercentageMultiplier * jdbc.ActiveSum / jdbc.MaxSum
		jdbcPtr = &v
	}

	return calculateAverage(nullableToSlice(sysPtr, hikariPtr, jdbcPtr, attrPtr)), nil
}

func (r *ClickHouseRepository) queryConnPoolMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	where := " AND " + infraconsts.ColServiceName + " = @serviceName"
	return r.queryConnPoolMetric(ctx, where, serviceParams(teamID, serviceName, startMs, endMs))
}

func (r *ClickHouseRepository) queryConnPoolMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	where := fmt.Sprintf(" AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName",
		infraconsts.ColHost, infraconsts.ColPod, infraconsts.ColContainer, infraconsts.ColServiceName)
	return r.queryConnPoolMetric(ctx, where, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs))
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
