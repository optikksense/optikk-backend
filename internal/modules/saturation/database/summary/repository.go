package summary

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"context"
	"fmt"
	"time"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

const metricsHistogramsRollupPrefix = "observability.metrics_histograms_rollup"

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
	fc, fargs := shared.FilterClauses(f)

	durationMs := max(float64(endMs-startMs)/1000.0, 1)

	// Percentiles + total count come from the pre-aggregated histogram rollup;
	// attribute filters (db.system / collection / namespace / server) were
	// applied at ingest on raw `observability.metrics` and are NOT preserved in
	// the rollup (`metrics_histograms_rollup` orders by team_id, bucket_ts,
	// metric_name, service). Those filters are now Go-ignored — the summary
	// card reports cross-system latency + ops. error_count stays on raw
	// because it requires the `error.type` attribute.
	mainTable, _ := rollup.TierTableFor(metricsHistogramsRollupPrefix, startMs, endMs)
	qMain := fmt.Sprintf(`
		SELECT
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1 AS p50,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3 AS p99,
		    sumMerge(hist_count)                                                AS total_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
	`, mainTable)

	rollupParams := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — tenant ID fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", shared.MetricDBOperationDuration),
	}

	var mainRaw summaryMainRawRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), qMain, rollupParams...).ScanStruct(&mainRaw); err != nil {
		return SummaryStats{}, err
	}
	mainDTO := summaryMainDTO{
		P50:        mainRaw.P50,
		P95:        mainRaw.P95,
		P99:        mainRaw.P99,
		TotalCount: int64(mainRaw.TotalCount), //nolint:gosec // domain-bounded
	}

	// error_count still needs raw `observability.metrics` — the `error.type`
	// attribute is not carried on the rollup. Narrow scan: histogram rows only.
	qErrors := fmt.Sprintf(`
		SELECT toInt64(sumIf(hist_count, notEmpty(%s))) AS error_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
	`,
		shared.AttrString(shared.AttrErrorType),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		fc,
	)
	var errorDTO struct {
		ErrorCount int64 `ch:"error_count"`
	}
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), qErrors, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...).ScanStruct(&errorDTO); err == nil {
		mainDTO.ErrorCount = errorDTO.ErrorCount
	}

	var errorRatePtr *float64
	if mainDTO.TotalCount > 0 {
		rate := float64(mainDTO.ErrorCount) / durationMs
		errorRatePtr = &rate
	}

	qConn := fmt.Sprintf(`
		SELECT toInt64(round(sum(value))) AS used_count
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

	var connDTO summaryConnDTO
	activeConns := int64(0)
	connArgs := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), qConn, connArgs...).ScanStruct(&connDTO); err == nil {
		activeConns = connDTO.UsedCount
	}

	qCache := fmt.Sprintf(`
		SELECT
		    toInt64(countIf(empty(%s))) AS success_count,
		    toInt64(count())            AS total_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND %s = 'redis'
		  AND metric_type = 'Histogram'
		  %s
	`,
		shared.AttrString(shared.AttrErrorType),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
		fc,
	)

	var cacheDTO summaryCacheDTO
	var cacheHitRate *float64
	cacheArgs := append(shared.BaseParams(teamID, startMs, endMs), fargs...)
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), qCache, cacheArgs...).ScanStruct(&cacheDTO); err == nil {
		if cacheDTO.TotalCount > 0 {
			rate := float64(cacheDTO.SuccessCount) / float64(cacheDTO.TotalCount) * 100
			cacheHitRate = &rate
		}
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
