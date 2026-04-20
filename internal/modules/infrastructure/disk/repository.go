package disk

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

// rescaleExpr emits a pure arithmetic rescale: if col<=1.0 multiply by 100,
// else leave unchanged. Uses boolean→UInt8 coercion so no if/multiIf is needed.
func rescaleExpr(col string) string {
	return fmt.Sprintf("(%s * (1 + 99 * (%s <= %.1f)))", col, col, infraconsts.PercentageThreshold)
}

// ---------------------------------------------------------------------------
// Bucket-series endpoints
// ---------------------------------------------------------------------------

// sumCountBucketRow is the scan target for directional/state/resource bucket
// endpoints. Avg is computed in Go via Sum/Count → avg is emitted as a nullable
// pointer when Count==0.
type sumCountBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	Key       string  `ch:"k"`
	Sum       float64 `ch:"s"`
	Count     uint64  `ch:"c"`
}

func (r *ClickHouseRepository) queryDirectionBuckets(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]directionBucketDTO, error) {
	b := bucket(startMs, endMs)
	dir := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemDiskDirection)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as k, sum(%s) as s, count() as c
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, dir, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, metricName)
	var rows []sumCountBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	out := make([]directionBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = DirectionBucket{
			Timestamp: row.Timestamp,
			Direction: row.Key,
			Value:     sumPtr(row.Sum),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetDiskIO(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBuckets(ctx, teamID, startMs, endMs, infraconsts.MetricSystemDiskIO)
}

func (r *ClickHouseRepository) GetDiskOperations(ctx context.Context, teamID int64, startMs, endMs int64) ([]directionBucketDTO, error) {
	return r.queryDirectionBuckets(ctx, teamID, startMs, endMs, infraconsts.MetricSystemDiskOperations)
}

func (r *ClickHouseRepository) GetDiskIOTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as k, sum(%s) as s, count() as c
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		b, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskIOTime)
	var rows []sumCountBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	out := make([]resourceBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = ResourceBucket{
			Timestamp: row.Timestamp,
			Pod:       "",
			Value:     sumPtr(row.Sum),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetFilesystemUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]mountpointBucketDTO, error) {
	b := bucket(startMs, endMs)
	mp := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrFilesystemMountpoint)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as k, sum(%s) as s, count() as c
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, mp, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemFilesystemUsage)
	var rows []sumCountBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	out := make([]mountpointBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = MountpointBucket{
			Timestamp:  row.Timestamp,
			Mountpoint: row.Key,
			Value:      divideOrNil(row.Sum, row.Count),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetFilesystemUtilization(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, '' as k, sum(%s) as s, count() as c
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1 ORDER BY 1`,
		b, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemFilesystemUtil)
	var rows []sumCountBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	out := make([]resourceBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = ResourceBucket{
			Timestamp: row.Timestamp,
			Pod:       "",
			Value:     divideOrNil(row.Sum, row.Count),
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

// sumCountRow is the canonical (sum,count) scan target so Go divides and
// emits a nullable pointer.
type sumCountRow struct {
	Sum   float64 `ch:"s"`
	Count uint64  `ch:"c"`
}

type ratioSumRow struct {
	FreeSum  float64 `ch:"free_sum"`
	TotalSum float64 `ch:"total_sum"`
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

// Instance-level filter predicates: plain OR-of-column-comparisons replace
// the previous coalesce/nullIf chain in SELECT. Callers supply real
// host/pod/container values (from the upstream listing endpoints).
const (
	hostMatchPredicate      = "(host = @host OR attributes.`host.name`::String = @host OR attributes.`server.address`::String = @host)"
	podMatchPredicate       = "(attributes.`k8s.pod.name`::String = @pod OR (@pod = '' AND attributes.`k8s.pod.name`::String = ''))"
	containerMatchPredicate = "((attributes.`container.name`::String = @container OR attributes.`k8s.container.name`::String = @container) OR (@container = '' AND attributes.`container.name`::String = '' AND attributes.`k8s.container.name`::String = ''))"
)

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	aDisk := infraconsts.AttrFloat(infraconsts.AttrSystemDiskUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s != ''
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )
		ORDER BY service_name`,
		infraconsts.ColServiceName,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColServiceName,
		infraconsts.ColMetricName, infraconsts.MetricSystemDiskUtilization, infraconsts.MetricDiskFree, infraconsts.MetricDiskTotal,
		aDisk)

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

// queryDiskMetric runs the three legs (system, free/total ratio, attribute)
// against a caller-supplied scope filter as parallel errgroup scans. Plain
// WHERE fragments + (sum, count) aggregates keep SELECT combinator-free; the
// mean is composed in Go.
func (r *ClickHouseRepository) queryDiskMetric(ctx context.Context, scopeWhere string, args []any) (*float64, error) {
	aDisk := infraconsts.AttrFloat(infraconsts.AttrSystemDiskUtilization)

	var (
		sys   sumCountRow
		attr  sumCountRow
		ratio ratioSumRow
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
			infraconsts.ColMetricName, infraconsts.MetricSystemDiskUtilization,
			infraconsts.ColValue,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&sys)
	})

	g.Go(func() error {
		// Sum free + total legs separately so Go can derive used% = 100*(1 - free/total).
		query := fmt.Sprintf(`
			SELECT
				sum(%s * (%s = '%s')) as free_sum,
				sum(%s * (%s = '%s')) as total_sum
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s IN ('%s', '%s')
			  AND %s >= 0
			  %s`,
			infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskFree,
			infraconsts.ColValue, infraconsts.ColMetricName, infraconsts.MetricDiskTotal,
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricDiskFree, infraconsts.MetricDiskTotal,
			infraconsts.ColValue,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&ratio)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s > 0
			  AND isFinite(%s)
			  %s`,
			rescaleExpr(aDisk),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			aDisk, aDisk,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&attr)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var sysPtr, ratioPtr, attrPtr *float64
	if sys.Count > 0 {
		v := sys.Sum / float64(sys.Count)
		sysPtr = &v
	}
	if attr.Count > 0 {
		v := attr.Sum / float64(attr.Count)
		attrPtr = &v
	}
	if ratio.TotalSum > 0 {
		v := infraconsts.PercentageMultiplier * (1.0 - (ratio.FreeSum / ratio.TotalSum))
		ratioPtr = &v
	}

	return calculateAverage(nullableToSlice(sysPtr, ratioPtr, attrPtr)), nil
}

func (r *ClickHouseRepository) queryDiskMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	where := " AND " + infraconsts.ColServiceName + " = @serviceName"
	return r.queryDiskMetric(ctx, where, serviceParams(teamID, serviceName, startMs, endMs))
}

func (r *ClickHouseRepository) queryDiskMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	where := fmt.Sprintf(" AND %s AND %s AND %s AND %s = @serviceName",
		hostMatchPredicate, podMatchPredicate, containerMatchPredicate, infraconsts.ColServiceName)
	return r.queryDiskMetric(ctx, where, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs))
}

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

func (r *ClickHouseRepository) GetDiskByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryDiskMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

func (r *ClickHouseRepository) GetDiskByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryDiskMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
