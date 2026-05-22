package log_trends //nolint:revive,stylecheck

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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
	TimeBucket     time.Time `ch:"time_bucket"`
	SeverityBucket uint8     `ch:"severity_bucket"`
	Count          uint64    `ch:"count"`
}

// Summary readers come in two shapes: with-CTE (when a resource-side
// predicate exists, narrowing fingerprints first) and bare (when no resource
// filter is set — active_fps would equal "every fingerprint in window" and
// the fingerprint-IN-clause would prune nothing). Granule pruning stays tight
// in the bare form via the (team_id, ts_bucket) leading PK columns.
//
// `timestamp BETWEEN` is in PREWHERE so CH uses the per-granule DateTime64
// min/max stat to prune within a bucket; explicit PREWHERE disables auto
// promotion. The condition is repeated in WHERE as the base for filter
// clauses to tack onto.
const summaryCTEHead = `
	WITH active_fps AS (
	    SELECT DISTINCT fingerprint
	    FROM observability.logs_resource
	    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`
const summaryCTETail = `
	)
	SELECT count()                       AS total,
	       countIf(severity_bucket >= 4) AS errors,
	       countIf(severity_bucket = 3)  AS warns
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
	     AND timestamp BETWEEN @start AND @end
	     AND fingerprint IN active_fps
	WHERE timestamp BETWEEN @start AND @end`
const summaryBareHead = `
	SELECT count()                       AS total,
	       countIf(severity_bucket >= 4) AS errors,
	       countIf(severity_bucket = 3)  AS warns
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
	     AND timestamp BETWEEN @start AND @end
	WHERE timestamp BETWEEN @start AND @end`

func (r *Repository) Summary(ctx context.Context, f filter.Filters) (SummaryRow, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	var query string
	if resourceWhere == "" {
		query = summaryBareHead + where
	} else {
		query = summaryCTEHead + resourceWhere + summaryCTETail + where
	}
	var row SummaryRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "logsTrends.Summary",
		&row, query, args...)
}

// Trend display grain is dispatched server-side via timebucket.DisplayGrainSQL
// to the specific toStartOf{Minute,FiveMinutes,Hour,Day} per window — 10–15%
// faster than the generic toStartOfInterval form for our 4 fixed grains.
//
// Mirrors Summary's shape: head ends at `WHERE timestamp BETWEEN …` so the
// row-side `where` from filter.BuildClauses (trace_id, span_id, severities,
// search, attributes) appends cleanly before GROUP BY/ORDER BY.
const trendCTEHead = `
	WITH active_fps AS (
	    SELECT DISTINCT fingerprint
	    FROM observability.logs_resource
	    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`

const trendGroupOrder = `
	GROUP BY time_bucket, severity_bucket
	ORDER BY time_bucket ASC, severity_bucket ASC`

func (r *Repository) Trend(ctx context.Context, f filter.Filters) ([]TrendRow, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	grainSQL := timebucket.DisplayGrainSQL(f.EndMs - f.StartMs)

	trendCTETail := `
	)
	SELECT ` + grainSQL + ` AS time_bucket,
	       severity_bucket,
	       count() AS count
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
	     AND timestamp BETWEEN @start AND @end
	     AND fingerprint IN active_fps
	WHERE timestamp BETWEEN @start AND @end`
	trendBareHead := `
	SELECT ` + grainSQL + ` AS time_bucket,
	       severity_bucket,
	       count() AS count
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
	     AND timestamp BETWEEN @start AND @end
	WHERE timestamp BETWEEN @start AND @end`

	var query string
	if resourceWhere == "" {
		query = trendBareHead + where + trendGroupOrder
	} else {
		query = trendCTEHead + resourceWhere + trendCTETail + where + trendGroupOrder
	}
	var rows []TrendRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "logsTrends.Trend",
		&rows, query, args...)
}
