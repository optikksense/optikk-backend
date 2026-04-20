package network

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

// rescaleExpr emits a pure arithmetic rescale: if col<=1.0 multiply by 100,
// else leave unchanged. Uses boolean→UInt8 coercion (no if/multiIf).
func rescaleExpr(col string) string {
	return fmt.Sprintf("(%s * (1 + 99 * (%s <= %.1f)))", col, col, infraconsts.PercentageThreshold)
}

// ---------------------------------------------------------------------------
// Bucket-series endpoints
// ---------------------------------------------------------------------------

// sumCountBucketRow is the scan target for directional/state/resource buckets.
// Go divides sum/count to emit a nullable pointer.
type sumCountBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	Key       string  `ch:"k"`
	Sum       float64 `ch:"s"`
	Count     uint64  `ch:"c"`
}

func (r *ClickHouseRepository) queryDirectionLikeSum(ctx context.Context, teamID int64, startMs, endMs int64, metricName, attrKey string) ([]sumCountBucketRow, error) {
	b := bucket(startMs, endMs)
	k := fmt.Sprintf("attributes.'%s'::String", attrKey)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as k, sum(%s) as s, count() as c
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, k, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, metricName)
	var rows []sumCountBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetNetworkIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	rows, err := r.queryDirectionLikeSum(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkIO, infraconsts.AttrSystemNetworkDirection)
	if err != nil {
		return nil, err
	}
	out := make([]directionBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = DirectionBucket{Timestamp: row.Timestamp, Direction: row.Key, Value: sumPtr(row.Sum)}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetNetworkPackets(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	rows, err := r.queryDirectionLikeSum(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkPackets, infraconsts.AttrSystemNetworkDirection)
	if err != nil {
		return nil, err
	}
	out := make([]directionBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = DirectionBucket{Timestamp: row.Timestamp, Direction: row.Key, Value: sumPtr(row.Sum)}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetNetworkErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	rows, err := r.queryDirectionLikeSum(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkErrors, infraconsts.AttrSystemNetworkDirection)
	if err != nil {
		return nil, err
	}
	out := make([]stateBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = StateBucket{Timestamp: row.Timestamp, State: row.Key, Value: sumPtr(row.Sum)}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetNetworkDropped(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as k, sum(%s) as s, count() as c
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		b, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemNetworkDropped)
	var rows []sumCountBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	out := make([]resourceBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = ResourceBucket{Timestamp: row.Timestamp, Pod: "", Value: sumPtr(row.Sum)}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetNetworkConnections(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	rows, err := r.queryDirectionLikeSum(ctx, teamID, startMs, endMs, infraconsts.MetricSystemNetworkConnections, infraconsts.AttrSystemNetworkState)
	if err != nil {
		return nil, err
	}
	out := make([]stateBucketDTO, len(rows))
	for i, row := range rows {
		// Connections is a gauge-type metric → return avg.
		out[i] = StateBucket{Timestamp: row.Timestamp, State: row.Key, Value: divideOrNil(row.Sum, row.Count)}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

type sumCountRow struct {
	Sum   float64 `ch:"s"`
	Count uint64  `ch:"c"`
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

func divideOrNil(sum float64, count uint64) *float64 {
	if count == 0 {
		return nil
	}
	v := sum / float64(count)
	return &v
}

func sumPtr(sum float64) *float64 {
	v := sum
	return &v
}

// Instance-level filter predicates — plain OR-of-column comparisons replace the
// previous coalesce/nullIf chain in SELECT.
const (
	hostMatchPredicate      = "(host = @host OR attributes.`host.name`::String = @host OR attributes.`server.address`::String = @host)"
	podMatchPredicate       = "(attributes.`k8s.pod.name`::String = @pod OR (@pod = '' AND attributes.`k8s.pod.name`::String = ''))"
	containerMatchPredicate = "((attributes.`container.name`::String = @container OR attributes.`k8s.container.name`::String = @container) OR (@container = '' AND attributes.`container.name`::String = '' AND attributes.`k8s.container.name`::String = ''))"
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

// queryNetworkMetric runs the two legs (system, attribute) in parallel and
// averages the non-null results Go-side. scopeWhere is the caller-supplied
// filter fragment; params include teamID, start, end + scope values.
func (r *ClickHouseRepository) queryNetworkMetric(ctx context.Context, scopeWhere string, args []any) (*float64, error) {
	aNet := infraconsts.AttrFloat(infraconsts.AttrSystemNetworkUtilization)

	var (
		sys  sumCountRow
		attr sumCountRow
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
			infraconsts.ColMetricName, infraconsts.MetricSystemNetworkUtilization,
			infraconsts.ColValue,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&sys)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s > 0
			  AND isFinite(%s)
			  %s`,
			rescaleExpr(aNet),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			aNet, aNet,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&attr)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var sysPtr, attrPtr *float64
	if sys.Count > 0 {
		v := sys.Sum / float64(sys.Count)
		sysPtr = &v
	}
	if attr.Count > 0 {
		v := attr.Sum / float64(attr.Count)
		attrPtr = &v
	}

	return calculateAverage(nullableToSlice(sysPtr, attrPtr)), nil
}

func (r *ClickHouseRepository) queryNetworkMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	where := " AND " + infraconsts.ColServiceName + " = @serviceName"
	return r.queryNetworkMetric(ctx, where, serviceParams(teamID, serviceName, startMs, endMs))
}

func (r *ClickHouseRepository) queryNetworkMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	where := fmt.Sprintf(" AND %s AND %s AND %s AND %s = @serviceName",
		hostMatchPredicate, podMatchPredicate, containerMatchPredicate, infraconsts.ColServiceName)
	return r.queryNetworkMetric(ctx, where, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs))
}

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

func (r *ClickHouseRepository) GetNetworkByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryNetworkMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

func (r *ClickHouseRepository) GetNetworkByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryNetworkMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
