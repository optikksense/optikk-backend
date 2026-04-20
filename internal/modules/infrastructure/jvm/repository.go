package jvm

import (
	"context"
	"fmt"
	"math"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
	"golang.org/x/sync/errgroup"
)

type Repository interface {
	GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketRow, error)
	GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmGCCollectionBucketDTO, error)
	GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmThreadBucketRow, error)
	GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsRow, error)
	GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsRow, error)
	GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func bucketExpr(startMs, endMs int64) string {
	return infraconsts.TimeBucketExpression(startMs, endMs)
}

// histogramSummaryRow carries the sum/count pair for histogram-backed metrics.
// Percentiles come from sketch.Querier (JvmMetricLatency).
type histogramSummaryRow struct {
	HistSum   float64 `ch:"hist_sum"`
	HistCount uint64  `ch:"hist_count"`
	P50       float64 `ch:"p50"`
	P95       float64 `ch:"p95"`
	P99       float64 `ch:"p99"`
}

// jvmMemoryBucketRow carries sum/count per memory kind so the service can
// compute avg in Go (replaces three avgIf() calls in SQL).
type jvmMemoryBucketRow struct {
	Timestamp      string
	PoolName       string
	MemType        string
	UsedSum        float64
	UsedCount      int64
	CommittedSum   float64
	CommittedCount int64
	LimitSum       float64
	LimitCount     int64
}

// jvmThreadBucketRow carries sum/count per (bucket, daemon).
type jvmThreadBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	Daemon    string  `ch:"daemon"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  uint64  `ch:"val_count"`
}

// jvmClassStatsRow carries totals; the service derives the final shape.
type jvmClassStatsRow struct {
	LoadedSum float64
	CountSum  float64
	CountN    int64
}

// jvmCPUStatsRow — cpu_time is a running total, cpu_util is an avg (sum/count).
type jvmCPUStatsRow struct {
	CPUTimeSum float64
	CPUUtilSum float64
	CPUUtilN   int64
}

// jvmBufferBucketRow carries sum/count pairs for memory_usage and count.
type jvmBufferBucketRow struct {
	Timestamp        string
	PoolName         string
	MemoryUsageSum   float64
	MemoryUsageCount int64
	BufCountSum      float64
	BufCountN        int64
}

// jvmMemoryRawRow is the raw (bucket, pool, mem_type) scan target for a single
// memory metric_name. One of these rows per (bucket, pool, type) tuple per leg.
type jvmMemoryRawRow struct {
	Timestamp string  `ch:"time_bucket"`
	PoolName  string  `ch:"pool_name"`
	MemType   string  `ch:"mem_type"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  uint64  `ch:"val_count"`
}

// fetchMemoryLeg runs one narrow-WHERE aggregation for a specific memory metric
// (used/committed/limit). Returns per-(bucket, pool, type) sum + count rows.
func (r *ClickHouseRepository) fetchMemoryLeg(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]jvmMemoryRawRow, error) {
	bucket := bucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMMemoryPoolName)
	memType := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMMemoryType)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			%s as mem_type,
			sum(%s)  as val_sum,
			count()  as val_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = @metricName
		GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`,
		bucket, pool, memType,
		infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", metricName),
	)
	var rows []jvmMemoryRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetJVMMemory runs three parallel narrow-WHERE scans (used/committed/limit)
// and merges them into (bucket, pool, mem_type) rows with per-metric sum+count.
// The service layer then computes the avg in Go.
func (r *ClickHouseRepository) GetJVMMemory(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmMemoryBucketRow, error) {
	var used, committed, limit []jvmMemoryRawRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		v, err := r.fetchMemoryLeg(gctx, teamID, startMs, endMs, infraconsts.MetricJVMMemoryUsed)
		used = v
		return err
	})
	g.Go(func() error {
		v, err := r.fetchMemoryLeg(gctx, teamID, startMs, endMs, infraconsts.MetricJVMMemoryCommitted)
		committed = v
		return err
	})
	g.Go(func() error {
		v, err := r.fetchMemoryLeg(gctx, teamID, startMs, endMs, infraconsts.MetricJVMMemoryLimit)
		limit = v
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	type key struct{ ts, pool, memType string }
	out := make(map[key]*jvmMemoryBucketRow)
	ensure := func(k key) *jvmMemoryBucketRow {
		row, ok := out[k]
		if !ok {
			row = &jvmMemoryBucketRow{Timestamp: k.ts, PoolName: k.pool, MemType: k.memType}
			out[k] = row
		}
		return row
	}
	for _, u := range used {
		row := ensure(key{u.Timestamp, u.PoolName, u.MemType})
		row.UsedSum = u.ValSum
		row.UsedCount = int64(u.ValCount) //nolint:gosec // count fits int64
	}
	for _, c := range committed {
		row := ensure(key{c.Timestamp, c.PoolName, c.MemType})
		row.CommittedSum = c.ValSum
		row.CommittedCount = int64(c.ValCount) //nolint:gosec // count fits int64
	}
	for _, l := range limit {
		row := ensure(key{l.Timestamp, l.PoolName, l.MemType})
		row.LimitSum = l.ValSum
		row.LimitCount = int64(l.ValCount) //nolint:gosec // count fits int64
	}

	// Emit sorted by (timestamp, pool, mem_type) to match the previous ORDER BY.
	rows := make([]jvmMemoryBucketRow, 0, len(out))
	for _, row := range out {
		rows = append(rows, *row)
	}
	sortMemoryRows(rows)
	return rows, nil
}

func sortMemoryRows(rows []jvmMemoryBucketRow) {
	// Simple 3-key sort matching the original SQL ORDER BY 1, 2, 3.
	// len is typically small (dozens of buckets × a handful of pools);
	// insertion-sort style via sort.Slice keeps the comparator inline.
	sortSlice3(rows)
}

func sortSlice3(rows []jvmMemoryBucketRow) {
	// Use stable sort by ts -> pool -> mem_type.
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			a, b := rows[j-1], rows[j]
			if lessMemoryRow(a, b) {
				break
			}
			rows[j-1], rows[j] = b, a
		}
	}
}

func lessMemoryRow(a, b jvmMemoryBucketRow) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	if a.PoolName != b.PoolName {
		return a.PoolName < b.PoolName
	}
	return a.MemType < b.MemType
}

func (r *ClickHouseRepository) GetJVMGCDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	// Quantiles + avg both come from hist_sum / hist_count roll-ups via
	// sketch.JvmMetricLatency. SQL only fetches the raw totals.
	query := fmt.Sprintf(`
		SELECT
			sum(hist_sum)   AS hist_sum,
			sum(hist_count) AS hist_count,
			0 AS p50, 0 AS p95, 0 AS p99
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = '%s' AND metric_type = 'Histogram'`,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	var row histogramSummaryRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return histogramSummaryRow{}, err
	}
	return row, nil
}

// jvmGCCollectionRawRow scans the raw per-(bucket, collector) hist_count total;
// the repository converts UInt64 → *float64 in Go.
type jvmGCCollectionRawRow struct {
	Timestamp string `ch:"time_bucket"`
	Collector string `ch:"collector"`
	SumCount  uint64 `ch:"sum_count"`
}

func (r *ClickHouseRepository) GetJVMGCCollections(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmGCCollectionBucketDTO, error) {
	bucket := bucketExpr(startMs, endMs)
	collector := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMGCName)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as collector, sum(hist_count) as sum_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = '%s' AND metric_type = 'Histogram'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, collector,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.MetricJVMGCDuration)
	var raw []jvmGCCollectionRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &raw, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	rows := make([]JVMGCCollectionBucket, len(raw))
	for i, r := range raw {
		v := float64(r.SumCount)
		rows[i] = JVMGCCollectionBucket{
			Timestamp: r.Timestamp,
			Collector: r.Collector,
			Value:     sanitizeFloatPtr(&v),
		}
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetJVMThreadCount(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmThreadBucketRow, error) {
	bucket := bucketExpr(startMs, endMs)
	daemon := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMThreadDaemon)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, %s as daemon,
		       sum(%s)  as val_sum,
		       count()  as val_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = '%s'
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, daemon, infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp, infraconsts.ColMetricName, infraconsts.MetricJVMThreadCount)
	var rows []jvmThreadBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// jvmClassLegRow scans one narrow-WHERE aggregation for a specific class metric.
type jvmClassLegRow struct {
	ValSum   float64 `ch:"val_sum"`
	ValCount uint64  `ch:"val_count"`
}

// fetchClassLeg returns sum+count for the given metric_name, filtered to finite
// values. isFinite stays in the WHERE clause (pure comparison, not banned).
func (r *ClickHouseRepository) fetchClassLeg(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (jvmClassLegRow, error) {
	query := fmt.Sprintf(`
		SELECT sum(%s) AS val_sum, count() AS val_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = @metricName AND isFinite(%s)`,
		infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName, infraconsts.ColValue)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", metricName),
	)
	var row jvmClassLegRow
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row)
	return row, err
}

// GetJVMClasses runs two parallel narrow-WHERE scans (loaded + count metric)
// and merges them into the final row.
func (r *ClickHouseRepository) GetJVMClasses(ctx context.Context, teamID int64, startMs, endMs int64) (jvmClassStatsRow, error) {
	var loaded, counts jvmClassLegRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		v, err := r.fetchClassLeg(gctx, teamID, startMs, endMs, infraconsts.MetricJVMClassLoaded)
		loaded = v
		return err
	})
	g.Go(func() error {
		v, err := r.fetchClassLeg(gctx, teamID, startMs, endMs, infraconsts.MetricJVMClassCount)
		counts = v
		return err
	})
	if err := g.Wait(); err != nil {
		return jvmClassStatsRow{}, err
	}
	return jvmClassStatsRow{
		LoadedSum: loaded.ValSum,
		CountSum:  counts.ValSum,
		CountN:    int64(counts.ValCount), //nolint:gosec // count fits int64
	}, nil
}

// jvmCPUSumRow scans a single sum(value) for the cpu_time leg.
type jvmCPUSumRow struct {
	ValSum float64 `ch:"val_sum"`
}

// jvmCPUSumCountRow scans sum+count for the cpu_util leg.
type jvmCPUSumCountRow struct {
	ValSum   float64 `ch:"val_sum"`
	ValCount uint64  `ch:"val_count"`
}

// GetJVMCPU runs two parallel narrow-WHERE scans: cpu_time total and
// cpu_util sum+count for the avg in Go.
func (r *ClickHouseRepository) GetJVMCPU(ctx context.Context, teamID int64, startMs, endMs int64) (jvmCPUStatsRow, error) {
	var cpuTime jvmCPUSumRow
	var cpuUtil jvmCPUSumCountRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT sum(%s) AS val_sum
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = @metricName`,
			infraconsts.ColValue,
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp, infraconsts.ColMetricName)
		args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
			clickhouse.Named("metricName", infraconsts.MetricJVMCPUTime))
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), q, args...).ScanStruct(&cpuTime)
	})
	g.Go(func() error {
		q := fmt.Sprintf(`
			SELECT sum(%s) AS val_sum, count() AS val_count
			FROM %s
			WHERE %s = @teamID AND %s BETWEEN @start AND @end AND %s = @metricName`,
			infraconsts.ColValue,
			infraconsts.TableMetrics,
			infraconsts.ColTeamID, infraconsts.ColTimestamp, infraconsts.ColMetricName)
		args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
			clickhouse.Named("metricName", infraconsts.MetricJVMCPUUtilization))
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), q, args...).ScanStruct(&cpuUtil)
	})
	if err := g.Wait(); err != nil {
		return jvmCPUStatsRow{}, err
	}
	return jvmCPUStatsRow{
		CPUTimeSum: cpuTime.ValSum,
		CPUUtilSum: cpuUtil.ValSum,
		CPUUtilN:   int64(cpuUtil.ValCount), //nolint:gosec // count fits int64
	}, nil
}

// jvmBufferRawRow carries one (bucket, pool) sum+count for a buffer metric.
type jvmBufferRawRow struct {
	Timestamp string  `ch:"time_bucket"`
	PoolName  string  `ch:"pool_name"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  uint64  `ch:"val_count"`
}

// fetchBufferLeg runs one narrow-WHERE aggregation per buffer metric.
func (r *ClickHouseRepository) fetchBufferLeg(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]jvmBufferRawRow, error) {
	bucket := bucketExpr(startMs, endMs)
	pool := fmt.Sprintf("attributes.'%s'::String", infraconsts.AttrJVMBufferPoolName)
	query := fmt.Sprintf(`
		SELECT
			%s as time_bucket,
			%s as pool_name,
			sum(%s)  as val_sum,
			count()  as val_count
		FROM %s
		WHERE %s = @teamID AND %s BETWEEN @start AND @end
		  AND %s = @metricName
		GROUP BY 1, 2 ORDER BY 1, 2`,
		bucket, pool,
		infraconsts.ColValue,
		infraconsts.TableMetrics,
		infraconsts.ColTeamID, infraconsts.ColTimestamp,
		infraconsts.ColMetricName)
	args := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", metricName),
	)
	var rows []jvmBufferRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetJVMBuffers runs two parallel narrow-WHERE scans (memory_usage + buffer
// count) and merges them into (bucket, pool) rows.
func (r *ClickHouseRepository) GetJVMBuffers(ctx context.Context, teamID int64, startMs, endMs int64) ([]jvmBufferBucketRow, error) {
	var memUsage, bufCount []jvmBufferRawRow
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		v, err := r.fetchBufferLeg(gctx, teamID, startMs, endMs, infraconsts.MetricJVMBufferMemoryUsage)
		memUsage = v
		return err
	})
	g.Go(func() error {
		v, err := r.fetchBufferLeg(gctx, teamID, startMs, endMs, infraconsts.MetricJVMBufferCount)
		bufCount = v
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	type key struct{ ts, pool string }
	out := make(map[key]*jvmBufferBucketRow)
	ensure := func(k key) *jvmBufferBucketRow {
		row, ok := out[k]
		if !ok {
			row = &jvmBufferBucketRow{Timestamp: k.ts, PoolName: k.pool}
			out[k] = row
		}
		return row
	}
	for _, m := range memUsage {
		row := ensure(key{m.Timestamp, m.PoolName})
		row.MemoryUsageSum = m.ValSum
		row.MemoryUsageCount = int64(m.ValCount) //nolint:gosec // count fits int64
	}
	for _, b := range bufCount {
		row := ensure(key{b.Timestamp, b.PoolName})
		row.BufCountSum = b.ValSum
		row.BufCountN = int64(b.ValCount) //nolint:gosec // count fits int64
	}

	rows := make([]jvmBufferBucketRow, 0, len(out))
	for _, row := range out {
		rows = append(rows, *row)
	}
	sortBufferRows(rows)
	return rows, nil
}

func sortBufferRows(rows []jvmBufferBucketRow) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			a, b := rows[j-1], rows[j]
			if lessBufferRow(a, b) {
				break
			}
			rows[j-1], rows[j] = b, a
		}
	}
}

func lessBufferRow(a, b jvmBufferBucketRow) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp < b.Timestamp
	}
	return a.PoolName < b.PoolName
}

func sanitizeFloat(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func sanitizeFloatPtr(v *float64) *float64 {
	if v == nil {
		return nil
	}
	if math.IsNaN(*v) || math.IsInf(*v, 0) {
		return nil
	}
	return v
}
