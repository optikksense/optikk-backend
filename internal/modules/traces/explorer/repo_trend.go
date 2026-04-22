package explorer

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"
)

// Trend computes a time-bucketed total + error count over the window,
// reading from traces_index.
func (r *Repository) Trend(ctx context.Context, f querycompiler.Filters) ([]TrendBucket, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetTracesIndex)
	bucketExpr := utils.ExprForColumn(f.StartMs, f.EndMs, "toDateTime(intDiv(start_ms, 1000))")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket, countIf(NOT has_error) AS total, countIf(has_error) AS errors
		FROM %s WHERE %s GROUP BY time_bucket ORDER BY time_bucket ASC`,
		bucketExpr, tracesIndexTable, compiled.Where,
	)
	rows, err := r.db.Query(dbutil.ExplorerCtx(ctx), query, compiled.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TrendBucket
	for rows.Next() {
		var (
			ts     string
			total  uint64
			errCnt uint64
		)
		if err := rows.Scan(&ts, &total, &errCnt); err != nil {
			return nil, err
		}
		out = append(out, TrendBucket{TimeBucket: ts, Total: total, Errors: errCnt})
	}
	return out, nil
}
