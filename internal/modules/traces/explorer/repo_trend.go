package explorer

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

// Trend computes a time-bucketed total + error count over the window. Reads
// raw spans with is_root = 1 and groups by the stored 5-min
// ts_bucket (no CH-side bucket math — see internal/infra/timebucket).
func (r *Repository) Trend(ctx context.Context, f querycompiler.Filters) ([]TrendBucket, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetSpansRaw)
	query := fmt.Sprintf(
		`SELECT formatDateTime(toDateTime(ts_bucket), '%%Y-%%m-%%d %%H:%%i:00') AS time_bucket,
		        countIf(NOT has_error) AS total, countIf(has_error) AS errors
		 FROM %s PREWHERE %s WHERE %s AND is_root = 1 GROUP BY time_bucket ORDER BY time_bucket ASC`,
		spansRawTable, compiled.PreWhere, compiled.Where,
	)
	rows, err := dbutil.QueryCH(dbutil.ExplorerCtx(ctx), r.db, "explorer.Trend", query, compiled.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TrendBucket
	for rows.Next() {
		var ts string
		var total, errCnt uint64
		if err := rows.Scan(&ts, &total, &errCnt); err != nil {
			return nil, err
		}
		out = append(out, TrendBucket{TimeBucket: ts, Total: total, Errors: errCnt})
	}
	return out, nil
}
