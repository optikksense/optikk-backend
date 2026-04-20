package explorer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	"golang.org/x/sync/errgroup"
)

const metricErrorRate = "error_rate"

// logStatsRepository issues strictly database-native SQL against
// observability.logs: pure SELECT + WHERE/PREWHERE + GROUP BY + count(),
// never `sumIf`, `countIf`, `toInt64`, `toString`, `if`, `multiIf`,
// `arrayJoin`, `arrayStringConcat`, `groupUniqArray`, or any combinator /
// cast / conditional. Every non-trivial transformation (severity→volume
// pivot, error-rate from (total, error) pair, int64 casts) lives in the
// service layer.
type logStatsRepository struct {
	db clickhouse.Conn
}

func newLogStatsRepository(db clickhouse.Conn) *logStatsRepository {
	return &logStatsRepository{db: db}
}

// facetDims is the list of `(name, column)` pairs GetLogStats fetches in
// parallel. Adding a dimension = appending one row; no SQL restructuring.
var facetDims = []struct {
	Name   string
	Column string
	Limit  int
}{
	{Name: "level", Column: "severity_text", Limit: 100},
	{Name: "service_name", Column: "service", Limit: 50},
	{Name: "host", Column: "host", Limit: 50},
	{Name: "pod", Column: "pod", Limit: 50},
	{Name: "scope_name", Column: "scope_name", Limit: 50},
}

func (r *logStatsRepository) GetLogHistogram(ctx context.Context, f shared.LogFilters, step string) ([]logHistogramRowDTO, error) {
	bucketExpr := shared.LogBucketExpr(f.StartMs, f.EndMs)
	if step != "" {
		bucketExpr = shared.LogBucketExprForStep(f.StartMs, f.EndMs, step)
	}
	prewhere, where, args := shared.BuildLogWhereSplit(f)
	whereClause := ""
	if where != "" {
		whereClause = " WHERE" + where
	}
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket, severity_text AS severity, count() AS count
		FROM observability.logs PREWHERE%s%s
		GROUP BY time_bucket, severity_text
		ORDER BY time_bucket ASC`, bucketExpr, prewhere, whereClause)

	var rows []logHistogramRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetLogVolume returns raw (bucket, severity, count) rows. Service pivots
// into the per-severity shape callers expect — keeps SQL pure count().
func (r *logStatsRepository) GetLogVolume(ctx context.Context, f shared.LogFilters, step string) ([]logVolumeRawRow, error) {
	bucketExpr := shared.LogBucketExpr(f.StartMs, f.EndMs)
	if step != "" {
		bucketExpr = shared.LogBucketExprForStep(f.StartMs, f.EndMs, step)
	}
	prewhere, where, args := shared.BuildLogWhereSplit(f)
	whereClause := ""
	if where != "" {
		whereClause = " WHERE" + where
	}
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket, severity_text, count() AS count
		FROM observability.logs PREWHERE%s%s
		GROUP BY time_bucket, severity_text
		ORDER BY time_bucket ASC`, bucketExpr, prewhere, whereClause)

	var rows []logVolumeRawRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetLogStats runs one query per facet dimension in parallel — each is a
// pure SELECT + GROUP BY + count(). The old single-query arrayJoin/multiIf
// form was fast but not native; 5 parallel native scans are slightly more
// round-trips to CH but each scan touches roughly the same data the
// arrayJoin path scanned once, and each is now cache-friendly at the CH
// query-plan level.
func (r *logStatsRepository) GetLogStats(ctx context.Context, f shared.LogFilters) ([]facetRowDTO, error) {
	prewhere, where, args := shared.BuildLogWhereSplit(f)

	var (
		mu  sync.Mutex
		out []facetRowDTO
	)

	g, gctx := errgroup.WithContext(ctx)
	for _, dim := range facetDims {
		dim := dim // capture loop variable
		g.Go(func() error {
			whereClause := " WHERE " + dim.Column + " != ''"
			if where != "" {
				whereClause = " WHERE" + where + " AND " + dim.Column + " != ''"
			}
			query := fmt.Sprintf(`
				SELECT %s AS value, count() AS count
				FROM observability.logs PREWHERE%s%s
				GROUP BY value
				ORDER BY count DESC
				LIMIT %d`, dim.Column, prewhere, whereClause, dim.Limit)

			var rows []facetScanRow
			if err := r.db.Select(dbutil.ExplorerCtx(gctx), &rows, query, args...); err != nil {
				return fmt.Errorf("logs: stats %s: %w", dim.Name, err)
			}

			mu.Lock()
			defer mu.Unlock()
			for _, row := range rows {
				out = append(out, facetRowDTO{
					Dim:   dim.Name,
					Value: row.Value,
					Count: int64(row.Count), //nolint:gosec // count is domain-bounded
				})
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *logStatsRepository) GetLogFields(ctx context.Context, f shared.LogFilters, col string) ([]valueCountRowDTO, error) {
	prewhere, where, args := shared.BuildLogWhereSplit(f)
	whereClause := " WHERE " + col + " != ''"
	if where != "" {
		whereClause = " WHERE" + where + " AND " + col + " != ''"
	}
	query := fmt.Sprintf(`
		SELECT %s AS value, count() AS count
		FROM observability.logs PREWHERE%s%s
		GROUP BY value
		ORDER BY count DESC
		LIMIT 200`, col, prewhere, whereClause)

	var rows []valueCountRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetTopGroups returns the top-N group values by count (or error-count when
// metric=error_rate). Error-rate ranking uses a narrow WHERE filter on
// severity_text — the predicate is a plain filter, not a combinator, which
// keeps the aggregate strictly count().
func (r *logStatsRepository) GetTopGroups(ctx context.Context, f shared.LogFilters, query logAggregateQuery) ([]topGroupRowDTO, error) {
	prewhere, where, args := shared.BuildLogWhereSplit(f)
	whereClause := " WHERE " + query.GroupCol + " != ''"
	if where != "" {
		whereClause = " WHERE" + where + " AND " + query.GroupCol + " != ''"
	}
	if query.Metric == metricErrorRate {
		whereClause += " AND severity_text IN ('ERROR', 'FATAL')"
	}

	sql := fmt.Sprintf(`
		SELECT %s AS grp, count() AS sort_value
		FROM observability.logs
		PREWHERE%s%s
		GROUP BY grp
		ORDER BY sort_value DESC
		LIMIT %d`, query.GroupCol, prewhere, whereClause, query.TopN)

	var rows []topGroupRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, sql, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetAggregateSeries returns per-(bucket, grp) counts. For error-rate
// metric, the service layer runs this twice (once for totals, once with a
// narrow severity_text IN filter for errors) and divides. SQL stays pure
// count() — zero if/sumIf/multiIf.
func (r *logStatsRepository) GetAggregateSeries(ctx context.Context, f shared.LogFilters, query logAggregateQuery, groups []string, onlyErrors bool) ([]logAggregateScanRow, error) {
	if len(groups) == 0 {
		return []logAggregateScanRow{}, nil
	}

	inPlaceholders := strings.Repeat("?,", len(groups))
	inPlaceholders = inPlaceholders[:len(inPlaceholders)-1]
	prewhere, where, args := shared.BuildLogWhereSplit(f)
	bucketExpr := shared.LogBucketExprForStep(f.StartMs, f.EndMs, query.Step)

	whereClause := fmt.Sprintf(" WHERE %s IN (%s)", query.GroupCol, inPlaceholders)
	if where != "" {
		whereClause = fmt.Sprintf(" WHERE%s AND %s IN (%s)", where, query.GroupCol, inPlaceholders)
	}
	if onlyErrors {
		whereClause += " AND severity_text IN ('ERROR', 'FATAL')"
	}

	combinedArgs := make([]any, 0, len(args)+len(groups))
	combinedArgs = append(combinedArgs, args...)
	combinedArgs = append(combinedArgs, stringSliceToAny(groups)...)

	sql := fmt.Sprintf(`
		SELECT %s AS time_bucket, %s AS grp, count() AS count
		FROM observability.logs
		PREWHERE%s%s
		GROUP BY time_bucket, grp
		ORDER BY time_bucket ASC, grp ASC`, bucketExpr, query.GroupCol, prewhere, whereClause)

	var rows []logAggregateScanRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, sql, combinedArgs...); err != nil {
		return nil, err
	}
	return rows, nil
}

func stringSliceToAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}
