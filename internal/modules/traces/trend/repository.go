package trend

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// Trend computes a time-bucketed total + error count over the window. Reads
// observability.spans_1m and groups by the window-adaptive display grain
// (1m ≤3h, 5m ≤24h, 1h ≤7d, 1d beyond) via timebucket.DisplayGrainSQL — the
// same server-side bucketing every other timeseries reader uses — so the chart
// granularity tracks the selected time range instead of a fixed 5-min grain.
func (r *Repository) Trend(ctx context.Context, f filter.Filters) ([]TrendBucket, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	grainSQL := timebucket.DisplayGrainSQL(f.EndMs - f.StartMs)

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT ` + grainSQL + `                          AS time_bucket,
		       sum(request_count) - sum(error_count)     AS total,
		       sum(error_count)                          AS errors
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where + `
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`

	var rows []struct {
		TimeBucket time.Time `ch:"time_bucket"`
		Total      int64     `ch:"total"`
		Errors     uint64    `ch:"errors"`
	}
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trend.Trend", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]TrendBucket, len(rows))
	for i, r := range rows {
		total := r.Total
		if total < 0 {
			total = 0
		}
		out[i] = TrendBucket{TimeBucket: timebucket.FormatDisplayBucket(r.TimeBucket), Total: uint64(total), Errors: r.Errors}
	}
	return out, nil
}
