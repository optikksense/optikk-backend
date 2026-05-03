package explorer

import (
	"context"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

// Trend computes a time-bucketed total + error count over the window. Reads
// raw spans with is_root = 1 and groups by the stored 5-min ts_bucket
// (no CH-side bucket math — see internal/infra/timebucket).
func (r *Repository) Trend(ctx context.Context, f filter.Filters) ([]TrendBucket, error) {
	resourceWhere, where, args := filter.BuildClauses(f)

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT toString(toDateTime(ts_bucket))                  AS time_bucket,
		       sum(request_count) - sum(error_count)            AS total,
		       sum(error_count)                                 AS errors
		FROM observability.spans_1m
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where + `
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`

	var rows []struct {
		TimeBucket string `ch:"time_bucket"`
		Total      uint64 `ch:"total"`
		Errors     uint64 `ch:"errors"`
	}
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.Trend", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]TrendBucket, len(rows))
	for i, r := range rows {
		out[i] = TrendBucket{TimeBucket: r.TimeBucket, Total: r.Total, Errors: r.Errors}
	}
	return out, nil
}
