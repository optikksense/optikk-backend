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

// TrendRow is one display-grain bucket carrying total and per-tier counts.
// Tier thresholds mirror the Summary query.
type TrendRow struct {
	TimeBucket time.Time `ch:"time_bucket"`
	Total      uint64    `ch:"total"`
	Error      uint64    `ch:"error"`
	Warn       uint64    `ch:"warn"`
	Info       uint64    `ch:"info"`
	Debug      uint64    `ch:"debug"`
}

// Summary reads use CTE to narrow fingerprints when resource predicates exist.
// Otherwise, it queries bare using leading PK columns for pruning.
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

// Trend queries aggregate countIf per severity tier into display buckets.
// Dispatched using adaptive display grain and supports trailing filters.
const trendCTEHead = `
	WITH active_fps AS (
	    SELECT DISTINCT fingerprint
	    FROM observability.logs_resource
	    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`

const trendGroupOrder = `
	GROUP BY time_bucket
	ORDER BY time_bucket ASC`

const trendSelectTail = ` AS time_bucket,
	       count()                       AS total,
	       countIf(severity_bucket >= 4) AS error,
	       countIf(severity_bucket = 3)  AS warn,
	       countIf(severity_bucket = 2)  AS info,
	       countIf(severity_bucket <= 1) AS debug`

func (r *Repository) Trend(ctx context.Context, f filter.Filters) ([]TrendRow, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	grainSQL := timebucket.DisplayGrainSQL(f.EndMs - f.StartMs)

	trendCTETail := `
	)
	SELECT ` + grainSQL + trendSelectTail + `
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
	     AND timestamp BETWEEN @start AND @end
	     AND fingerprint IN active_fps
	WHERE timestamp BETWEEN @start AND @end`
	trendBareHead := `
	SELECT ` + grainSQL + trendSelectTail + `
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
