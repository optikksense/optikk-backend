package log_trends //nolint:revive,stylecheck

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// SummaryRow folds the severity-bucket counts SQL-side. Convention:
// 0=trace..5=fatal; 3=warn, 4+=error.
type SummaryRow struct {
	Total  uint64 `ch:"total"`
	Errors uint64 `ch:"errors"`
	Warns  uint64 `ch:"warns"`
}

// TrendRawRow is the per-log row for service-side bucket-format + fold.
type TrendRawRow struct {
	Timestamp      time.Time `ch:"timestamp"`
	SeverityBucket uint8     `ch:"severity_bucket"`
}

// Summary returns SQL-side aggregated counts for the list-header KPIs.
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

// Trend returns raw (timestamp, severity_bucket) pairs; service folds into
// step-aligned buckets.
func (r *Repository) Trend(ctx context.Context, f filter.Filters) ([]TrendRawRow, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.logs_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT timestamp, severity_bucket
		FROM observability.logs
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where + `
		ORDER BY timestamp ASC`
	var rows []TrendRawRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsTrends.Trend",
		&rows, query, args...)
}
