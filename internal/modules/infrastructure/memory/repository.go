package memory

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
	"golang.org/x/sync/errgroup"
)

type Repository interface {
	GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error)
	GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error)
	GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error)
	GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error)
	GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error)
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

// rescaleExpr returns a SQL expression that converts a 0-1 fraction into a
// 0-100 percentage (leaving already-0-100 values unchanged). The CH-native
// boolean-to-UInt8 coercion keeps the emitted SQL free of conditional
// builtins: when `col <= 1.0` resolves to 1 the multiplier is 100; when it
// resolves to 0 the multiplier is 1. Pure per-row expression.
func rescaleExpr(col string) string {
	return fmt.Sprintf("(%s * (1 + 99 * (%s <= %.1f)))", col, col, infraconsts.PercentageThreshold)
}

// sumCountRow is the CH scan target for the simple (sum, count) aggregate
// shape used by the system-util, attribute, and JVM legs below. Plain
// division-in-Go keeps the emitted SQL aggregate-combinator-free.
type sumCountRow struct {
	Sum   float64 `ch:"s"`
	Count uint64  `ch:"c"`
}

// stateBucketSumCountRow aliases sumCountRow with the time/state key columns
// so the swap/memory-usage endpoints return plain (bucket, state, sum, count)
// rows and the service/dto layer computes the avg.
type stateBucketSumCountRow struct {
	Timestamp string  `ch:"time_bucket"`
	State     string  `ch:"state"`
	Sum       float64 `ch:"metric_sum"`
	Count     uint64  `ch:"metric_count"`
}

func (r *ClickHouseRepository) queryStateBuckets(ctx context.Context, query string, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	var rows []stateBucketSumCountRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	out := make([]stateBucketDTO, len(rows))
	for i, row := range rows {
		out[i] = StateBucket{
			Timestamp: row.Timestamp,
			State:     row.State,
			Value:     divideOrNil(row.Sum, row.Count),
		}
	}
	return out, nil
}

func divideOrNil(sum float64, count uint64) *float64 {
	if count == 0 {
		return nil
	}
	v := sum / float64(count)
	return &v
}

func (r *ClickHouseRepository) GetMemoryUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_sum, count() as metric_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUsage)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) GetSwapUsage(ctx context.Context, teamID int64, startMs, endMs int64) ([]stateBucketDTO, error) {
	b := bucket(startMs, endMs)
	state := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrSystemMemoryState)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as state, sum(%s) as metric_sum, count() as metric_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		b, state, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricSystemPagingUsage)
	return r.queryStateBuckets(ctx, query, teamID, startMs, endMs)
}

// ---------------------------------------------------------------------------
// Percentage-view scans (per-bucket per-service)
// ---------------------------------------------------------------------------

// pctBucketSysRow is the per-(bucket, service) system-memory-utilization leg.
// Sum is the rescale-then-sum of `value` so Go computes (Sum/Count) without
// a SQL-side average function.
type pctBucketSysRow struct {
	Timestamp string  `ch:"time_bucket"`
	Service   string  `ch:"pod"`
	Sum       float64 `ch:"s"`
	Count     uint64  `ch:"c"`
}

// pctBucketAttrRow mirrors pctBucketSysRow for the attribute-based leg.
type pctBucketAttrRow = pctBucketSysRow

// pctBucketJvmRow is the per-(bucket, service, metric_name) JVM leg. Emitting
// metric_name in the scan output lets Go split jvm.memory.used from
// jvm.memory.max without per-metric combinators.
type pctBucketJvmRow struct {
	Timestamp  string  `ch:"time_bucket"`
	Service    string  `ch:"pod"`
	MetricName string  `ch:"metric_name"`
	Sum        float64 `ch:"s"`
}

func pctKey(bucket, service string) string { return bucket + "|" + service }

func (r *ClickHouseRepository) GetMemoryUsagePercentage(ctx context.Context, teamID int64, startMs, endMs int64) ([]resourceBucketDTO, error) {
	b := bucket(startMs, endMs)
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)

	var (
		sysRows  []pctBucketSysRow
		jvmRows  []pctBucketJvmRow
		attrRows []pctBucketAttrRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s as time_bucket, %s as pod,
			       sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s = '%s'
			  AND isFinite(%s)
			  AND %s >= 0
			GROUP BY 1, 2
			HAVING pod != ''`,
			b, infraconsts.ColServiceName,
			rescaleExpr(infraconsts.ColValue),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization,
			infraconsts.ColValue,
			infraconsts.ColValue)
		return r.db.Select(dbutil.OverviewCtx(gctx), &sysRows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s as time_bucket, %s as pod, %s as metric_name,
			       sum(%s) as s
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s IN ('%s', '%s')
			  AND isFinite(%s)
			  AND %s >= 0
			GROUP BY 1, 2, 3
			HAVING pod != ''`,
			b, infraconsts.ColServiceName, infraconsts.ColMetricName,
			infraconsts.ColValue,
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
			infraconsts.ColValue,
			infraconsts.ColValue)
		return r.db.Select(dbutil.OverviewCtx(gctx), &jvmRows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s as time_bucket, %s as pod,
			       sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s > 0
			  AND isFinite(%s)
			GROUP BY 1, 2
			HAVING pod != ''`,
			b, infraconsts.ColServiceName,
			rescaleExpr(aMem),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			aMem, aMem)
		return r.db.Select(dbutil.OverviewCtx(gctx), &attrRows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	type cell struct {
		sys, attr                *float64
		jvmUsedSum, jvmMaxSum    float64
		jvmUsedSeen, jvmMaxSeen  bool
	}
	keys := make([]string, 0, len(sysRows)+len(attrRows))
	seen := make(map[string]struct{}, len(sysRows)+len(attrRows))
	cells := make(map[string]*cell, len(sysRows)+len(attrRows))
	getOrInit := func(ts, svc string) *cell {
		k := pctKey(ts, svc)
		if c, ok := cells[k]; ok {
			return c
		}
		c := &cell{}
		cells[k] = c
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
		return c
	}

	sysKeyMeta := make(map[string][2]string)
	for _, row := range sysRows {
		if row.Count == 0 {
			continue
		}
		c := getOrInit(row.Timestamp, row.Service)
		v := row.Sum / float64(row.Count)
		c.sys = &v
		sysKeyMeta[pctKey(row.Timestamp, row.Service)] = [2]string{row.Timestamp, row.Service}
	}
	for _, row := range attrRows {
		if row.Count == 0 {
			continue
		}
		c := getOrInit(row.Timestamp, row.Service)
		v := row.Sum / float64(row.Count)
		c.attr = &v
		sysKeyMeta[pctKey(row.Timestamp, row.Service)] = [2]string{row.Timestamp, row.Service}
	}
	for _, row := range jvmRows {
		c := getOrInit(row.Timestamp, row.Service)
		switch row.MetricName {
		case infraconsts.MetricJVMMemoryUsed:
			c.jvmUsedSum = row.Sum
			c.jvmUsedSeen = true
		case infraconsts.MetricJVMMemoryMax:
			c.jvmMaxSum = row.Sum
			c.jvmMaxSeen = true
		}
		sysKeyMeta[pctKey(row.Timestamp, row.Service)] = [2]string{row.Timestamp, row.Service}
	}

	type sortEntry struct {
		ts, svc, key string
	}
	entries := make([]sortEntry, 0, len(keys))
	for _, k := range keys {
		meta := sysKeyMeta[k]
		entries = append(entries, sortEntry{ts: meta[0], svc: meta[1], key: k})
	}
	// Stable order: timestamp asc, service asc (mirrors the original ORDER BY).
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].ts != entries[j].ts {
			return entries[i].ts < entries[j].ts
		}
		return entries[i].svc < entries[j].svc
	})

	out := make([]resourceBucketDTO, 0, len(entries))
	for _, e := range entries {
		c := cells[e.key]
		var jvm *float64
		if c.jvmUsedSeen && c.jvmMaxSeen && c.jvmMaxSum > 0 {
			v := infraconsts.PercentageMultiplier * c.jvmUsedSum / c.jvmMaxSum
			jvm = &v
		}
		combined := calculateAverage(nullableToSlice(c.sys, jvm, c.attr))
		out = append(out, ResourceBucket{
			Timestamp: e.ts,
			Pod:       e.svc,
			Value:     combined,
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Avg / by-service / by-instance helpers
// ---------------------------------------------------------------------------

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

// Instance-level filter predicates: replace the previous fallback-resolution
// chain with plain OR-of-column-comparisons. The display-time 'unknown'
// sentinel never reaches the filter because callers get real host/pod/
// container values from the upstream listing endpoints.
const (
	hostMatchPredicate = "(host = @host OR attributes.`host.name`::String = @host OR attributes.`server.address`::String = @host)"
	podMatchPredicate  = "(attributes.`k8s.pod.name`::String = @pod OR (@pod = '' AND attributes.`k8s.pod.name`::String = ''))"
	containerMatchPredicate = "((attributes.`container.name`::String = @container OR attributes.`k8s.container.name`::String = @container) OR (@container = '' AND attributes.`container.name`::String = '' AND attributes.`k8s.container.name`::String = ''))"
)

// serviceListBanned-free query: returns distinct service names that have at
// least one memory-relevant sample.
func (r *ClickHouseRepository) getServiceList(ctx context.Context, teamID int64, startMs, endMs int64) ([]string, error) {
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)
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
		infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
		aMem)

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

// jvmMetricRow carries a per-metric_name sum for the JVM leg; Go aggregates
// jvm.memory.used and jvm.memory.max into ratio = 100 * used_sum / max_sum.
type jvmMetricRow struct {
	MetricName string  `ch:"metric_name"`
	Sum        float64 `ch:"s"`
}

// queryMemoryMetric runs the three legs (system, JVM, attribute) against a
// caller-supplied scope (service-only or instance-level) in parallel and
// averages the non-null results. The caller supplies the WHERE fragment and
// named parameters; this function handles the rest uniformly.
func (r *ClickHouseRepository) queryMemoryMetric(ctx context.Context, scopeWhere string, args []any) (*float64, error) {
	aMem := infraconsts.AttrFloat(infraconsts.AttrSystemMemoryUtilization)

	var (
		sys  sumCountRow
		attr sumCountRow
		jvm  []jvmMetricRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s = '%s'
			  AND isFinite(%s)
			  AND %s >= 0
			  %s`,
			rescaleExpr(infraconsts.ColValue),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricSystemMemoryUtilization,
			infraconsts.ColValue, infraconsts.ColValue,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&sys)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s as metric_name, sum(%s) as s
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s IN ('%s', '%s')
			  AND isFinite(%s)
			  AND %s >= 0
			  %s
			GROUP BY 1`,
			infraconsts.ColMetricName, infraconsts.ColValue,
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			infraconsts.ColMetricName, infraconsts.MetricJVMMemoryUsed, infraconsts.MetricJVMMemoryMax,
			infraconsts.ColValue, infraconsts.ColValue,
			scopeWhere)
		return r.db.Select(dbutil.OverviewCtx(gctx), &jvm, query, args...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT sum(%s) as s, count() as c
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end
			  AND %s > 0
			  AND isFinite(%s)
			  %s`,
			rescaleExpr(aMem),
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp,
			aMem, aMem,
			scopeWhere)
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&attr)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var sysPtr, jvmPtr, attrPtr *float64
	if sys.Count > 0 {
		v := sys.Sum / float64(sys.Count)
		sysPtr = &v
	}
	if attr.Count > 0 {
		v := attr.Sum / float64(attr.Count)
		attrPtr = &v
	}
	var jvmUsedSum, jvmMaxSum float64
	var usedSeen, maxSeen bool
	for _, row := range jvm {
		switch row.MetricName {
		case infraconsts.MetricJVMMemoryUsed:
			jvmUsedSum = row.Sum
			usedSeen = true
		case infraconsts.MetricJVMMemoryMax:
			jvmMaxSum = row.Sum
			maxSeen = true
		}
	}
	if usedSeen && maxSeen && jvmMaxSum > 0 {
		v := infraconsts.PercentageMultiplier * jvmUsedSum / jvmMaxSum
		jvmPtr = &v
	}

	return calculateAverage(nullableToSlice(sysPtr, jvmPtr, attrPtr)), nil
}

func (r *ClickHouseRepository) queryMemoryMetricByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	where := " AND " + infraconsts.ColServiceName + " = @serviceName"
	return r.queryMemoryMetric(ctx, where, serviceParams(teamID, serviceName, startMs, endMs))
}

func (r *ClickHouseRepository) queryMemoryMetricByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	where := fmt.Sprintf(" AND %s AND %s AND %s AND %s = @serviceName",
		hostMatchPredicate, podMatchPredicate, containerMatchPredicate, infraconsts.ColServiceName)
	return r.queryMemoryMetric(ctx, where, instanceParams(teamID, host, pod, container, serviceName, startMs, endMs))
}

func (r *ClickHouseRepository) GetAvgMemory(ctx context.Context, teamID int64, startMs, endMs int64) (metricValueDTO, error) {
	services, err := r.getServiceList(ctx, teamID, startMs, endMs)
	if err != nil {
		return MetricValue{Value: 0}, err
	}

	var values []float64
	for _, svc := range services {
		memVal, err := r.queryMemoryMetricByService(ctx, teamID, svc, startMs, endMs)
		if err == nil && memVal != nil && *memVal >= 0 {
			values = append(values, *memVal)
		}
	}

	avg := calculateAverage(values)
	if avg == nil {
		return MetricValue{Value: 0}, nil
	}
	return MetricValue{Value: *avg}, nil
}

func (r *ClickHouseRepository) GetMemoryByService(ctx context.Context, teamID int64, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryMemoryMetricByService(ctx, teamID, serviceName, startMs, endMs)
}

func (r *ClickHouseRepository) GetMemoryByInstance(ctx context.Context, teamID int64, host, pod, container, serviceName string, startMs, endMs int64) (*float64, error) {
	return r.queryMemoryMetricByInstance(ctx, teamID, host, pod, container, serviceName, startMs, endMs)
}
