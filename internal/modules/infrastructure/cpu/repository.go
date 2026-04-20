package cpu

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

func bucket(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

// rescaleExpr emits a pure arithmetic rescale: if col<=1.0 multiply by 100,
// else leave unchanged. Uses boolean→UInt8 coercion (no if/multiIf).
func rescaleExpr(col string) string {
	return fmt.Sprintf("(%s * (1 + 99 * (%s <= %.1f)))", col, col, infraconsts.PercentageThreshold)
}

// sumCountRow is the canonical (sum,count) scan target for leg aggregates.
type sumCountRow struct {
	Sum   float64 `ch:"s"`
	Count uint64  `ch:"c"`
}

// stateBucketSumCountRow pairs (bucket,state) with sum+count so the service
// divides Go-side.
type stateBucketSumCountRow struct {
	Timestamp string  `ch:"time_bucket"`
	State     string  `ch:"state"`
	Sum       float64 `ch:"metric_sum"`
	Count     uint64  `ch:"metric_count"`
}

// pctBucketRow is the per-(bucket, service) leg row for the usage-percentage
// scans. Sum is the rescaled-then-summed value; Go divides by Count.
type pctBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	Service   string  `ch:"pod"`
	Sum       float64 `ch:"s"`
	Count     uint64  `ch:"c"`
}

// loadLegRow holds one leg of the load-average split scan.
type loadLegRow struct {
	Sum   float64 `ch:"s"`
	Count uint64  `ch:"c"`
}

// bucketMeta carries the (timestamp, service) pair alongside the composite
// bucket-key used by the usage-percentage scan reducer.
type bucketMeta struct {
	ts, svc string
}

func divideOrNil(sum float64, count uint64) *float64 {
	if count == 0 {
		return nil
	}
	v := sum / float64(count)
	return &v
}

// ---------------------------------------------------------------------------
// CPU time / process count (bucket series)
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64, isAvg bool) ([]stateBucketDTO, error) {
	var rows []stateBucketSumCountRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	out := make([]stateBucketDTO, len(rows))
	for i, row := range rows {
		var v *float64
		if isAvg {
			v = divideOrNil(row.Sum, row.Count)
		} else {
			// CPU time is a sum-type metric.
			tmp := row.Sum
			v = &tmp
		}
		out[i] = StateBucket{Timestamp: row.Timestamp, State: row.State, Value: v}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetCPUTime(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemCPUState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_sum, count() as metric_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUTime)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs, false)
}

func (r *ClickHouseRepository) GetProcessCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	status := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrProcessStatus)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_sum, count() as metric_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, status, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemProcessCount)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs, true)
}

// ---------------------------------------------------------------------------
// CPU usage percentage (bucketed, per-pod)
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetCPUUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)

	var (
		sysRows     []pctBucketRow
		processRows []pctBucketRow
		attrRows    []pctBucketRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s as time_bucket, %s as pod,
			       sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s IN ('%s', '%s')
			  AND isFinite(%s)
			  AND %s >= 0
			  AND %s <= %.1f
			GROUP BY 1, 2
			HAVING pod != ''`,
			b, infraconsts.ColServiceName,
			rescaleExpr(infraconsts.ColValue),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage,
			infraconsts.ColValue,
			infraconsts.ColValue,
			infraconsts.ColValue, infraconsts.PercentageThreshold)
		return r.db.Select(dbutil.OverviewCtx(gctx), &sysRows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s as time_bucket, %s as pod,
			       sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s = '%s'
			  AND isFinite(%s)
			  AND %s >= 0
			  AND %s <= %.1f
			GROUP BY 1, 2
			HAVING pod != ''`,
			b, infraconsts.ColServiceName,
			rescaleExpr(infraconsts.ColValue),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage,
			infraconsts.ColValue,
			infraconsts.ColValue,
			infraconsts.ColValue, infraconsts.PercentageThreshold)
		return r.db.Select(dbutil.OverviewCtx(gctx), &processRows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s as time_bucket, %s as pod,
			       sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s >= 0
			  AND %s <= %.1f
			GROUP BY 1, 2
			HAVING pod != ''`,
			b, infraconsts.ColServiceName,
			rescaleExpr(aCPU),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			aCPU,
			aCPU, infraconsts.PercentageThreshold)
		return r.db.Select(dbutil.OverviewCtx(gctx), &attrRows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	type cell struct {
		sys, process, attr *float64
	}

	cells := make(map[string]*cell)
	meta := make(map[string]bucketMeta)
	keys := make([]string, 0)

	getOrInit := func(ts, svc string) *cell {
		k := ts + "|" + svc
		if c, ok := cells[k]; ok {
			return c
		}
		c := &cell{}
		cells[k] = c
		meta[k] = bucketMeta{ts: ts, svc: svc}
		keys = append(keys, k)
		return c
	}

	applyAvg := func(rows []pctBucketRow, which int) {
		for _, row := range rows {
			if row.Count == 0 {
				continue
			}
			c := getOrInit(row.Timestamp, row.Service)
			v := row.Sum / float64(row.Count)
			switch which {
			case 0:
				c.sys = &v
			case 1:
				c.process = &v
			case 2:
				c.attr = &v
			}
		}
	}
	applyAvg(sysRows, 0)
	applyAvg(processRows, 1)
	applyAvg(attrRows, 2)

	// Sort by (timestamp asc, service asc) to mirror the original ORDER BY.
	sortedKeys := sortBucketKeys(keys, meta)

	out := make([]resourceBucketDTO, 0, len(sortedKeys))
	for _, k := range sortedKeys {
		c := cells[k]
		m := meta[k]
		combined := calculateAverage(nullableToSlice(c.sys, c.process, c.attr))
		out = append(out, ResourceBucket{
			Timestamp: m.ts,
			Pod:       m.svc,
			Value:     combined,
		})
	}
	return out, nil
}

func sortBucketKeys(keys []string, meta map[string]bucketMeta) []string {
	// Simple insertion sort keeps a tiny dependency-free path; N is bucket * services.
	result := make([]string, len(keys))
	copy(result, keys)
	for i := 1; i < len(result); i++ {
		j := i
		for j > 0 {
			a, b := meta[result[j-1]], meta[result[j]]
			if a.ts < b.ts || (a.ts == b.ts && a.svc <= b.svc) {
				break
			}
			result[j-1], result[j] = result[j], result[j-1]
			j--
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Load average (split scan, 3 legs)
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetLoadAverage(ctx context.Context, teamID int64, startMs, endMs int64) (loadAverageResultDTO, error) {
	var (
		leg1  loadLegRow
		leg5  loadLegRow
		leg15 loadLegRow
	)
	g, gctx := errgroup.WithContext(ctx)

	runLeg := func(metric string, dest *loadLegRow) func() error {
		return func() error {
			query := fmt.Sprintf(`
				SELECT sum(%s) as s, count() as c
				FROM %s
				WHERE %s = @teamID AND %s BETWEEN @start AND @end
				  AND %s = '%s'`,
				infraconsts.ColValue,
				infraconsts.TableMetrics,
				infraconsts.ColTeamID, infraconsts.ColTimestamp,
				infraconsts.ColMetricName, metric)
			return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(dest)
		}
	}
	g.Go(runLeg(infraconsts.MetricSystemCPULoadAvg1m, &leg1))
	g.Go(runLeg(infraconsts.MetricSystemCPULoadAvg5m, &leg5))
	g.Go(runLeg(infraconsts.MetricSystemCPULoadAvg15m, &leg15))

	if err := g.Wait(); err != nil {
		return loadAverageResultDTO{}, err
	}

	pickAvg := func(l loadLegRow) float64 {
		if l.Count == 0 {
			return 0
		}
		return l.Sum / float64(l.Count)
	}

	return loadAverageResultDTO{
		Load1m:  pickAvg(leg1),
		Load5m:  pickAvg(leg5),
		Load15m: pickAvg(leg15),
	}, nil
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

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

// queryCPUMetric runs the three legs (system, process, attribute) as parallel
// errgroup scans with plain WHERE fragments + (sum,count) aggregates.
// scopeWhere is caller-supplied (service- or instance-level).
func (r *ClickHouseRepository) queryCPUMetric(ctx context.Context, scopeWhere string, args []any) (*float64, error) {
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)

	var (
		sys     sumCountRow
		process sumCountRow
		attr    sumCountRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s IN ('%s', '%s')
			  AND isFinite(%s)
			  AND %s >= 0
			  AND %s <= %.1f
			  %s`,
			rescaleExpr(infraconsts.ColValue),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage,
			infraconsts.ColValue,
			infraconsts.ColValue,
			infraconsts.ColValue, infraconsts.PercentageThreshold,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&sys)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s = '%s'
			  AND isFinite(%s)
			  AND %s >= 0
			  AND %s <= %.1f
			  %s`,
			rescaleExpr(infraconsts.ColValue),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricProcessCPUUsage,
			infraconsts.ColValue,
			infraconsts.ColValue,
			infraconsts.ColValue, infraconsts.PercentageThreshold,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&process)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s >= 0
			  AND %s <= %.1f
			  %s`,
			rescaleExpr(aCPU),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			aCPU,
			aCPU, infraconsts.PercentageThreshold,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&attr)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var sysPtr, processPtr, attrPtr *float64
	if sys.Count > 0 {
		v := sys.Sum / float64(sys.Count)
		sysPtr = &v
	}
	if process.Count > 0 {
		v := process.Sum / float64(process.Count)
		processPtr = &v
	}
	if attr.Count > 0 {
		v := attr.Sum / float64(attr.Count)
		attrPtr = &v
	}

	return calculateAverage(nullableToSlice(sysPtr, processPtr, attrPtr)), nil
}

func (r *ClickHouseRepository) queryCPUMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	where := " AND " + infraconsts.ColServiceName + " = @serviceName"
	return r.queryCPUMetric(ctx, where, serviceParams(teamID, serviceName, startMs, endMs))
}

func (r *ClickHouseRepository) queryCPUMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	where := fmt.Sprintf(" AND %s = @host AND %s = @pod AND %s = @container AND %s = @serviceName",
		infraconsts.ColHost, infraconsts.ColPod, infraconsts.ColContainer, infraconsts.ColServiceName)
	return r.queryCPUMetric(ctx, where, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs))
}

func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)
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
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)

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
	aCPU := infraconsts.AttrFloat(infraconsts.AttrSystemCPUUtilization)
	query := fmt.Sprintf(`
		SELECT DISTINCT %s as host, %s as pod, %s as container, %s as service_name
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s != ''
		  AND (
		      %s IN ('%s', '%s', '%s')
		      OR %s > 0
		  )
		LIMIT 200`,
		infraconsts.ColHost, infraconsts.ColPod, infraconsts.ColContainer, infraconsts.ColServiceName,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColServiceName,
		infraconsts.ColMetricName, infraconsts.MetricSystemCPUUtilization, infraconsts.MetricSystemCPUUsage, infraconsts.MetricProcessCPUUsage,
		aCPU)

	var rows []instanceRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) GetAvgCPU(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, service := range services {
		cpuVal, err := r.queryCPUMetricByService(ctx, teamID, service, startMs, endMs)
		if err == nil && cpuVal != nil && *cpuVal >= 0 {
			values = append(values, *cpuVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetCPUByService(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuServiceMetricDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	result := make([]cpuServiceMetricDTO, len(services))
	for i, serviceName := range services {
		cpuVal, _ := r.queryCPUMetricByService(ctx, teamID, serviceName, startMs, endMs)
		result[i] = cpuServiceMetricDTO{
			ServiceName: serviceName,
			Value:       cpuVal,
		}
	}
	return result, nil
}

func (r *ClickHouseRepository) GetCPUByInstance(ctx context.Context, teamID int64, startMs, endMs int64) ([]cpuInstanceMetricDTO, error) {
	instances, err := r.getInstanceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}

	result := make([]cpuInstanceMetricDTO, len(instances))
	for i, inst := range instances {
		cpuVal, _ := r.queryCPUMetricByInstance(ctx, teamID, inst.Host, inst.Pod, inst.Container, inst.ServiceName, startMs, endMs)
		result[i] = cpuInstanceMetricDTO{
			Host:        inst.Host,
			Pod:         inst.Pod,
			Container:   inst.Container,
			ServiceName: inst.ServiceName,
			Value:       cpuVal,
		}
	}
	return result, nil
}
