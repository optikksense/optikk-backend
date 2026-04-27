package summary

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"golang.org/x/sync/errgroup"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
		shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (SummaryStats, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// summaryMainRawRow mirrors summaryMainDTO but scans the UInt64 counts that
// `sumMerge` emits; the service layer consumes the DTO with int64 totals.
type summaryMainRawRow struct {
	P50        *float64 `ch:"p50"`
	P95        *float64 `ch:"p95"`
	P99        *float64 `ch:"p99"`
	TotalCount uint64   `ch:"total_count"`
}

func (r *ClickHouseRepository) GetSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (SummaryStats, error) {
	durationMs := max(float64(endMs-startMs)/1000.0, 1)
	filterFrag, filterArgs := shared.RollupFilterClauses(f)

	mainTable := ""
	dbHistTable := ""

	baseParams := func(metricArg any) []any {
		out := []any{
			clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — tenant ID fits uint32
			clickhouse.Named("start", time.UnixMilli(startMs)),
			clickhouse.Named("end", time.UnixMilli(endMs)),
			metricArg,
		}
		out = append(out, filterArgs...)
		return out
	}

	// Percentiles + total count from the pre-aggregated histogram rollup.
	qMain := fmt.Sprintf(`
		SELECT
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_digest)[1]) AS p50,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_digest)[2]) AS p95,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_digest)[3]) AS p99,
		    sum(hist_count)                                                AS total_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName%s
	`, mainTable, filterFrag)

	qErrors := fmt.Sprintf(`
		SELECT toInt64(sum(hist_count)) AS error_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND error_type != ''%s
	`, dbHistTable, filterFrag)

	qConn := fmt.Sprintf(`
		SELECT toInt64(round(toFloat64(sum(value_sum)))) AS used_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND db_connection_state = 'used'%s
	`, dbHistTable, filterFrag)

	qCache := fmt.Sprintf(`
		SELECT
		    toInt64(sum(hist_count))                             AS total_count,
		    toInt64(sumMergeIf(hist_count, error_type = ''))          AS success_count
		FROM %s
		WHERE team_id = @teamID
		  AND ts_bucket BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND db_system = 'redis'%s
	`, dbHistTable, filterFrag)

	var (
		mainRaw  summaryMainRawRow
		errorRow struct {
			ErrorCount int64 `ch:"error_count"`
		}
		connDTO  summaryConnDTO
		cacheDTO summaryCacheDTO
		errorOK  bool
		connOK   bool
		cacheOK  bool
	)

	g, gctx := errgroup.WithContext(dbutil.DashboardCtx(ctx))
	opMetric := clickhouse.Named("metricName", shared.MetricDBOperationDuration)

	g.Go(func() error {
		return dbutil.QueryRowCH(gctx, r.db, "summary.main", &mainRaw, qMain, baseParams(opMetric)...)
	})
	g.Go(func() error {
		if err := dbutil.QueryRowCH(gctx, r.db, "summary.errors", &errorRow, qErrors, baseParams(opMetric)...); err == nil {
			errorOK = true
		}
		return nil
	})
	g.Go(func() error {
		connMetric := clickhouse.Named("metricName", shared.MetricDBConnectionCount)
		if err := dbutil.QueryRowCH(gctx, r.db, "summary.conn", &connDTO, qConn, baseParams(connMetric)...); err == nil {
			connOK = true
		}
		return nil
	})
	g.Go(func() error {
		if err := dbutil.QueryRowCH(gctx, r.db, "summary.cache", &cacheDTO, qCache, baseParams(opMetric)...); err == nil {
			cacheOK = true
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return SummaryStats{}, err
	}

	mainDTO := summaryMainDTO{
		P50:        mainRaw.P50,
		P95:        mainRaw.P95,
		P99:        mainRaw.P99,
		TotalCount: int64(mainRaw.TotalCount), //nolint:gosec // domain-bounded
	}
	if errorOK {
		mainDTO.ErrorCount = errorRow.ErrorCount
	}

	var errorRatePtr *float64
	if mainDTO.TotalCount > 0 {
		rate := float64(mainDTO.ErrorCount) / durationMs
		errorRatePtr = &rate
	}

	var activeConns int64
	if connOK {
		activeConns = connDTO.UsedCount
	}

	var cacheHitRate *float64
	if cacheOK && cacheDTO.TotalCount > 0 {
		rate := float64(cacheDTO.SuccessCount) / float64(cacheDTO.TotalCount) * 100
		cacheHitRate = &rate
	}

	return SummaryStats{
		AvgLatencyMs:      shared.ScaleToMs(mainDTO.P50),
		P95LatencyMs:      shared.ScaleToMs(mainDTO.P95),
		P99LatencyMs:      shared.ScaleToMs(mainDTO.P99),
		SpanCount:         mainDTO.TotalCount,
		ActiveConnections: activeConns,
		ErrorRate:         errorRatePtr,
		CacheHitRate:      cacheHitRate,
	}, nil
}
