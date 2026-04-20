package summary

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetSummaryMain(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryMainDTO, error)
	GetSummaryConn(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryConnDTO, error)
	GetSummaryCache(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryCacheDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// summaryTotalsDTO is the totals leg of GetSummaryMain: one scan over all
// histogram rows for this tenant. ErrorCount is filled by the errors leg
// merge.
type summaryTotalsDTO struct {
	LatencySum   float64 `ch:"latency_sum"`
	LatencyCount uint64  `ch:"latency_count"`
	TotalCount   uint64  `ch:"total_count"`
}

type summaryErrorsDTO struct {
	ErrorCount uint64 `ch:"error_count"`
}

// GetSummaryMain returns counters + raw sum/count for in-Go avg latency.
// Percentiles (p50/p95/p99) are attached by the service layer from the
// DbOpLatency sketch. The prior `sumIf` combinator is split into two
// parallel scans — the error leg adds `AND notEmpty(error_type)` to the
// WHERE clause so the aggregate stays a pure sum(hist_count).
func (r *ClickHouseRepository) GetSummaryMain(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryMainDTO, error) {
	fc, fargs := shared.FilterClauses(f)
	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	totalsQuery := fmt.Sprintf(`
		SELECT
		    sum(hist_sum)    AS latency_sum,
		    sum(hist_count)  AS latency_count,
		    sum(hist_count)  AS total_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
	`,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)

	errorsQuery := fmt.Sprintf(`
		SELECT
		    sum(hist_count)  AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
	`,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrErrorType),
		fc,
	)

	var (
		totals summaryTotalsDTO
		errs   summaryErrorsDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), totalsQuery, params...).ScanStruct(&totals)
	})
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), errorsQuery, params...).ScanStruct(&errs)
	})
	if err := g.Wait(); err != nil {
		return summaryMainDTO{}, err
	}

	return summaryMainDTO{
		LatencySum:   totals.LatencySum,
		LatencyCount: totals.LatencyCount,
		TotalCount:   totals.TotalCount,
		ErrorCount:   errs.ErrorCount,
	}, nil
}

// GetSummaryConn returns sum/count for gauge `value` so the service can
// compute average active connections in Go (no SQL avg()).
func (r *ClickHouseRepository) GetSummaryConn(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryConnDTO, error) {
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    sum(value)   AS used_sum,
		    count()      AS used_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND %s = 'used'
		  %s
	`,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionCount,
		shared.AttrString(shared.AttrConnectionState),
		fc,
	)

	var dto summaryConnDTO
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...).ScanStruct(&dto)
	return dto, err
}

type summaryCacheLegDTO struct {
	N uint64 `ch:"n"`
}

// GetSummaryCache returns (success_count, total_count) for redis histograms.
// The previous `countIf(empty(error_type))` is split into a totals scan and
// a success scan (`AND empty(error_type)` in WHERE); each returns a pure
// count().
func (r *ClickHouseRepository) GetSummaryCache(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryCacheDTO, error) {
	fc, fargs := shared.FilterClauses(f)
	params := append(shared.BaseParams(teamID, startMs, endMs), fargs...)

	totalsQuery := fmt.Sprintf(`
		SELECT count() AS n
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND %s = 'redis'
		  AND metric_type = 'Histogram'
		  %s
	`,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		fc,
	)

	successQuery := fmt.Sprintf(`
		SELECT count() AS n
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND %s = 'redis'
		  AND metric_type = 'Histogram'
		  AND empty(%s)
		  %s
	`,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		shared.AttrString(shared.AttrErrorType),
		fc,
	)

	var (
		totals  summaryCacheLegDTO
		success summaryCacheLegDTO
	)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), totalsQuery, params...).ScanStruct(&totals)
	})
	g.Go(func() error {
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), successQuery, params...).ScanStruct(&success)
	})
	if err := g.Wait(); err != nil {
		return summaryCacheDTO{}, err
	}

	return summaryCacheDTO{
		SuccessCount: success.N,
		TotalCount:   totals.N,
	}, nil
}
