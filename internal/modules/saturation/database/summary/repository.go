package summary

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (SummaryStats, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (SummaryStats, error) {
	fc, fargs := shared.FilterClauses(f)

	durationMs := max(float64(endMs-startMs)/1000.0, 1)

	qMain := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count)  AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count)  AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count)  AS p99,
		    toInt64(sum(hist_count))                                                    AS total_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))                                    AS error_count
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

	var mainDTO summaryMainDTO
	if err := r.db.QueryRow(ctx, &mainDTO, qMain, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...); err != nil {
		return SummaryStats{}, err
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
	`,
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBConnectionCount,
		shared.AttrString(shared.AttrConnectionState),
	)

	var connDTO summaryConnDTO
	activeConns := int64(0)
	if err := r.db.QueryRow(ctx, &connDTO, qConn, shared.BaseParams(teamID, startMs, endMs)...); err == nil {
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
	`,
		shared.AttrString(shared.AttrErrorType),
		shared.TableMetrics,
		shared.ColTeamID, shared.ColTimestamp,
		shared.ColMetricName, shared.MetricDBOperationDuration,
		shared.AttrString(shared.AttrDBSystem),
	)

	var cacheDTO summaryCacheDTO
	var cacheHitRate *float64
	if err := r.db.QueryRow(ctx, &cacheDTO, qCache, shared.BaseParams(teamID, startMs, endMs)...); err == nil {
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
