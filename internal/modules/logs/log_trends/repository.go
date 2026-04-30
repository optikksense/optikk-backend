package log_trends //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

type SummaryRow struct {
	Total  uint64 `ch:"total"`
	Errors uint64 `ch:"errors"`
	Warns  uint64 `ch:"warns"`
}

type TrendRow struct {
	TsBucket       uint32 `ch:"ts_bucket"`
	SeverityBucket uint8  `ch:"severity_bucket"`
	Count          uint64 `ch:"count"`
}

func (r *Repository) Summary(ctx context.Context, f filter.Filters) (SummaryRow, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.logs_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT count()                              AS total,
		       countIf(severity_bucket >= 4)        AS errors,
		       countIf(severity_bucket = 3)         AS warns
		FROM observability.logs
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where
	var row SummaryRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "logsTrends.Summary",
		&row, query, args...)
}

func (r *Repository) Trend(ctx context.Context, f filter.Filters) ([]TrendRow, error) {
	resourceWhere, _, args := filter.BuildClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.logs_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT ts_bucket, severity_bucket, count() AS count
		FROM observability.logs
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		GROUP BY ts_bucket, severity_bucket
		ORDER BY ts_bucket ASC, severity_bucket ASC`
	var rows []TrendRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsTrends.Trend",
		&rows, query, args...)
}
