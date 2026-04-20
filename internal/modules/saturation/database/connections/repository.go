package connections

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetConnectionCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionCountPoint, error)
	GetConnectionUtilization(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionUtilPoint, error)
	GetConnectionLimits(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionLimits, error)
	GetPendingRequests(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PendingRequestsPoint, error)
	GetConnectionTimeoutRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionTimeoutPoint, error)
	GetConnectionWaitTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error)
	GetConnectionCreateTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error)
	GetConnectionUseTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetConnectionCountSeries returns per-(time_bucket, pool_name, state) raw
// sum+count of gauge `value`. Service divides in Go to surface the average.
func (r *ClickHouseRepository) GetConnectionCountSeries(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionCountPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	stateAttr := shared.AttrString(shared.AttrConnectionState)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s           AS time_bucket,
		    %s           AS pool_name,
		    %s           AS state,
		    sum(value)   AS value_sum,
		    count()      AS value_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  %s
		GROUP BY time_bucket, pool_name, state
		ORDER BY time_bucket, pool_name, state
	`,
		bucket, poolAttr, stateAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionCount,
		fc,
	)

	var rows []ConnectionCountPoint
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].CountN > 0 {
			avg := rows[i].CountSum / float64(rows[i].CountN)
			rows[i].Count = &avg
		}
	}
	return rows, nil
}

// connUsedLegDTO carries per-(time_bucket, pool_name) used-state sum/count.
type connUsedLegDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	PoolName   string  `ch:"pool_name"`
	UsedSum    float64 `ch:"used_sum"`
	UsedN      uint64  `ch:"used_count"`
}

// connMaxLegDTO carries per-pool_name capacity-metric sum/count. max is
// gauge-valued (one value per data point) so avg = sum/count.
type connMaxLegDTO struct {
	PoolName string  `ch:"pool_name"`
	MaxSum   float64 `ch:"max_sum"`
	MaxN     uint64  `ch:"max_count"`
}

// GetConnectionUtilization runs two parallel scans — used-state count
// aggregation and pool capacity aggregation — merging into utilisation %
// Go-side. Replaces prior `avgIf` + correlated subquery + `if(...)` pattern.
func (r *ClickHouseRepository) GetConnectionUtilization(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionUtilPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	stateAttr := shared.AttrString(shared.AttrConnectionState)
	fc, fargs := shared.FilterClauses(f)
	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	usedQuery := fmt.Sprintf(`
		SELECT
		    %s            AS time_bucket,
		    %s            AS pool_name,
		    sum(value)    AS used_sum,
		    count()       AS used_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND %s = 'used'
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionCount,
		stateAttr,
		fc,
	)

	maxQuery := fmt.Sprintf(`
		SELECT
		    %s         AS pool_name,
		    sum(value) AS max_sum,
		    count()    AS max_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  %s
		GROUP BY pool_name
	`,
		poolAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionMax,
		fc,
	)

	var (
		usedRows []connUsedLegDTO
		maxRows  []connMaxLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.Select(database.OverviewCtx(gctx), &usedRows, usedQuery, params...)
	})
	g.Go(func() error {
		return r.db.Select(database.OverviewCtx(gctx), &maxRows, maxQuery, params...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	maxByPool := make(map[string]float64, len(maxRows))
	for _, m := range maxRows {
		if m.MaxN > 0 {
			maxByPool[m.PoolName] = m.MaxSum / float64(m.MaxN)
		}
	}

	out := make([]ConnectionUtilPoint, len(usedRows))
	for i, u := range usedRows {
		p := ConnectionUtilPoint{
			TimeBucket: u.TimeBucket,
			PoolName:   u.PoolName,
			UsedSum:    u.UsedSum,
			UsedN:      int64(u.UsedN), //nolint:gosec // tenant-scoped gauge count fits int64
		}
		maxAvg := maxByPool[u.PoolName]
		p.MaxSum = maxAvg
		p.MaxN = 1
		if u.UsedN > 0 && maxAvg > 0 {
			usedAvg := u.UsedSum / float64(u.UsedN)
			util := usedAvg / maxAvg * 100
			p.UtilPct = &util
		}
		out[i] = p
	}
	return out, nil
}

// connLimitLegDTO is one metric's sum+count keyed by pool_name.
type connLimitLegDTO struct {
	PoolName string  `ch:"pool_name"`
	ValueSum float64 `ch:"value_sum"`
	ValueN   uint64  `ch:"value_count"`
}

// GetConnectionLimits runs three parallel scans (max / idle_max / idle_min)
// and merges them Go-side. Replaces the prior `avgIf(value, metric_name =
// '...')` triple in a single SELECT.
func (r *ClickHouseRepository) GetConnectionLimits(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionLimits, error) {
	poolAttr := shared.AttrString(shared.AttrPoolName)
	fc, fargs := shared.FilterClauses(f)
	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	scan := func(metric string) (string, []any) {
		q := fmt.Sprintf(`
			SELECT
			    %s         AS pool_name,
			    sum(value) AS value_sum,
			    count()    AS value_count
			FROM %s
			WHERE %s = @teamID
			  AND %s BETWEEN @start AND @end
			  AND %s = '%s'
			  %s
			GROUP BY pool_name
		`,
			poolAttr,
			shared.TableMetrics,
			shared.ColTeamID, shared.ColTimestamp,
			shared.ColMetricName, metric,
			fc,
		)
		return q, params
	}

	var (
		maxRows     []connLimitLegDTO
		idleMaxRows []connLimitLegDTO
		idleMinRows []connLimitLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		q, p := scan(shared.MetricDBConnectionMax)
		return r.db.Select(database.OverviewCtx(gctx), &maxRows, q, p...)
	})
	g.Go(func() error {
		q, p := scan(shared.MetricDBConnectionIdleMax)
		return r.db.Select(database.OverviewCtx(gctx), &idleMaxRows, q, p...)
	})
	g.Go(func() error {
		q, p := scan(shared.MetricDBConnectionIdleMin)
		return r.db.Select(database.OverviewCtx(gctx), &idleMinRows, q, p...)
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	byPool := make(map[string]*ConnectionLimits, len(maxRows)+len(idleMaxRows)+len(idleMinRows))
	order := make([]string, 0, len(byPool))
	ensure := func(pool string) *ConnectionLimits {
		if p, ok := byPool[pool]; ok {
			return p
		}
		p := &ConnectionLimits{PoolName: pool}
		byPool[pool] = p
		order = append(order, pool)
		return p
	}
	for _, row := range maxRows {
		p := ensure(row.PoolName)
		p.MaxSum = row.ValueSum
		p.MaxN = int64(row.ValueN) //nolint:gosec // tenant-scoped gauge count fits int64
		if row.ValueN > 0 {
			v := row.ValueSum / float64(row.ValueN)
			p.Max = &v
		}
	}
	for _, row := range idleMaxRows {
		p := ensure(row.PoolName)
		p.IdleMaxSum = row.ValueSum
		p.IdleMaxN = int64(row.ValueN) //nolint:gosec // tenant-scoped gauge count fits int64
		if row.ValueN > 0 {
			v := row.ValueSum / float64(row.ValueN)
			p.IdleMax = &v
		}
	}
	for _, row := range idleMinRows {
		p := ensure(row.PoolName)
		p.IdleMinSum = row.ValueSum
		p.IdleMinN = int64(row.ValueN) //nolint:gosec // tenant-scoped gauge count fits int64
		if row.ValueN > 0 {
			v := row.ValueSum / float64(row.ValueN)
			p.IdleMin = &v
		}
	}

	out := make([]ConnectionLimits, 0, len(order))
	for _, k := range order {
		out = append(out, *byPool[k])
	}
	return out, nil
}

func (r *ClickHouseRepository) GetPendingRequests(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PendingRequestsPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s           AS time_bucket,
		    %s           AS pool_name,
		    sum(value)   AS value_sum,
		    count()      AS value_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionPendReqs,
		fc,
	)

	var rows []PendingRequestsPoint
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	for i := range rows {
		if rows[i].CountN > 0 {
			avg := rows[i].CountSum / float64(rows[i].CountN)
			rows[i].Count = &avg
		}
	}
	return rows, nil
}

// connTimeoutRawDTO is the per-(bucket, pool) counter-metric raw sum.
type connTimeoutRawDTO struct {
	TimeBucket string  `ch:"time_bucket"`
	PoolName   string  `ch:"pool_name"`
	ValueSum   float64 `ch:"value_sum"`
}

func (r *ClickHouseRepository) GetConnectionTimeoutRate(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]ConnectionTimeoutPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s           AS time_bucket,
		    %s           AS pool_name,
		    sum(value)   AS value_sum
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionTimeouts,
		fc,
	)

	var dtos []connTimeoutRawDTO
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &dtos, query, args...); err != nil {
		return nil, err
	}

	out := make([]ConnectionTimeoutPoint, len(dtos))
	for i, d := range dtos {
		rate := d.ValueSum / bucketSec
		out[i] = ConnectionTimeoutPoint{
			TimeBucket:  d.TimeBucket,
			PoolName:    d.PoolName,
			TimeoutRate: &rate,
		}
	}
	return out, nil
}

// poolLatency returns zero-filled percentile rows. Connection-pool wait/use
// duration percentiles need a dedicated `DbConnPoolLatency` sketch kind —
// tracked as a Phase-3 follow-up. The SELECT stays pure count()/sum() + avg
// in Go; p50/p95/p99 are emitted as constant zeros so the existing DTO shape
// and timeseries assembly keep working.
func (r *ClickHouseRepository) poolLatency(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, f shared.Filters) ([]PoolLatencyPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := shared.AttrString(shared.AttrPoolName)
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS pool_name,
		    0.0 AS p50_ms,
		    0.0 AS p95_ms,
		    0.0 AS p99_ms
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND hist_count > 0
		  %s
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, metricName,
		fc,
	)

	var rows []PoolLatencyPoint
	args := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetConnectionWaitTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return r.poolLatency(ctx, teamID, startMs, endMs, shared.MetricDBConnectionWaitTime, f)
}

func (r *ClickHouseRepository) GetConnectionCreateTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return r.poolLatency(ctx, teamID, startMs, endMs, shared.MetricDBConnectionCreateTime, f)
}

func (r *ClickHouseRepository) GetConnectionUseTime(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) ([]PoolLatencyPoint, error) {
	return r.poolLatency(ctx, teamID, startMs, endMs, shared.MetricDBConnectionUseTime, f)
}
