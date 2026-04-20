package summary

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"

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

// GetSummaryMain returns counters + raw sum/count for in-Go avg latency.
// Percentiles (p50/p95/p99) are attached by the service layer from the
// DbOpLatency sketch — zero quantile() calls here.
func (r *ClickHouseRepository) GetSummaryMain(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryMainDTO, error) {
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    sum(hist_sum)                                AS latency_sum,
		    toInt64(sum(hist_count))                     AS latency_count,
		    toInt64(sum(hist_count))                     AS total_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))     AS error_count
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

	var dto summaryMainDTO
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...).ScanStruct(&dto)
	return dto, err
}

// GetSummaryConn returns sum/count for gauge `value` so the service can
// compute average active connections in Go (no SQL avg()).
func (r *ClickHouseRepository) GetSummaryConn(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryConnDTO, error) {
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    sum(value)               AS used_sum,
		    toInt64(count())         AS used_count
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

func (r *ClickHouseRepository) GetSummaryCache(ctx context.Context, teamID int64, startMs, endMs int64, f shared.Filters) (summaryCacheDTO, error) {
	fc, fargs := shared.FilterClauses(f)

	query := fmt.Sprintf(`
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

	var dto summaryCacheDTO
	err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, append(shared.BaseParams(teamID, startMs, endMs), fargs...)...).ScanStruct(&dto)
	return dto, err
}
