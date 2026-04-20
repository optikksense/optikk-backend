package explorer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

const (
	metricErrorRate = "error_rate"
	logsRollupPrefix = "observability.logs_rollup"
)

// queryIntervalMinutes returns max(tierStep, dashboardStep) for the query-time
// group-by. Copied from overview/overview/repository.go.
func queryIntervalMinutes(tierStepMin int64, startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	var dashStep int64
	switch {
	case hours <= 3:
		dashStep = 1
	case hours <= 24:
		dashStep = 5
	case hours <= 168:
		dashStep = 60
	default:
		dashStep = 1440
	}
	if tierStepMin > dashStep {
		return tierStepMin
	}
	return dashStep
}

type logStatsRepository struct {
	db clickhouse.Conn
}

func newLogStatsRepository(db clickhouse.Conn) *logStatsRepository {
	return &logStatsRepository{db: db}
}

// buildRollupLogWhere produces a WHERE clause compatible with the logs
// rollup — only filters on rollup key columns (severity_text, service,
// host, pod) survive. Filters that require the raw log body / attributes
// (Search, TraceID, SpanID, AttributeFilters, Containers, Environments)
// are silently dropped when the query is migrated to the rollup.
func buildRollupLogWhere(f shared.LogFilters) (where string, args []any) {
	const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000
	startMs := f.StartMs
	endMs := f.EndMs
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}

	where = ` team_id = ? AND bucket_ts BETWEEN ? AND ?`
	args = []any{
		uint32(f.TeamID), //nolint:gosec // G115 - tenant id fits uint32
		time.UnixMilli(startMs),
		time.UnixMilli(endMs),
	}

	appendInClause := func(column string, values []string, negated bool) {
		if len(values) == 0 {
			return
		}
		if negated {
			where += ` AND ` + column + ` NOT IN (?)`
		} else {
			where += ` AND ` + column + ` IN (?)`
		}
		args = append(args, values)
	}

	appendInClause("severity_text", f.Severities, false)
	appendInClause("service", f.Services, false)
	appendInClause("host", f.Hosts, false)
	appendInClause("pod", f.Pods, false)
	appendInClause("severity_text", f.ExcludeSeverities, true)
	appendInClause("service", f.ExcludeServices, true)
	appendInClause("host", f.ExcludeHosts, true)

	return where, args
}

// rollupBucketExpr returns the SQL expression for time-bucketing the rollup
// `bucket_ts` column at `stepMin`-minute resolution, formatted for return.
func rollupBucketExpr(stepMin int64) string {
	return fmt.Sprintf("formatDateTime(toStartOfInterval(bucket_ts, toIntervalMinute(%d)), '%%Y-%%m-%%d %%H:%%i:00')", stepMin)
}

func (r *logStatsRepository) GetLogHistogram(ctx context.Context, f shared.LogFilters, step string) ([]logHistogramRowDTO, error) {
	table, tierStep := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
	stepMin := resolveStepMinutes(step, tierStep, f.StartMs, f.EndMs)
	bucketExpr := rollupBucketExpr(stepMin)

	where, args := buildRollupLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       severity_text AS severity,
		       sumMerge(log_count) AS count_u
		FROM %s
		WHERE%s
		GROUP BY time_bucket, severity_text
		ORDER BY time_bucket ASC`, bucketExpr, table, where)

	type rowU struct {
		TimeBucket string `ch:"time_bucket"`
		Severity   string `ch:"severity"`
		CountU     uint64 `ch:"count_u"`
	}
	var raw []rowU
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}
	rows := make([]logHistogramRowDTO, len(raw))
	for i, row := range raw {
		rows[i] = logHistogramRowDTO{
			TimeBucket: row.TimeBucket,
			Severity:   row.Severity,
			Count:      int64(row.CountU), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

func (r *logStatsRepository) GetLogVolume(ctx context.Context, f shared.LogFilters, step string) ([]logVolumeRowDTO, error) {
	table, tierStep := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
	stepMin := resolveStepMinutes(step, tierStep, f.StartMs, f.EndMs)
	bucketExpr := rollupBucketExpr(stepMin)

	where, args := buildRollupLogWhere(f)
	// Rollup only has (log_count, error_count) — per-severity totals are
	// reconstructed by grouping on severity_text and pivoting in Go.
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       severity_text AS severity,
		       sumMerge(log_count) AS count_u
		FROM %s
		WHERE%s
		GROUP BY time_bucket, severity_text
		ORDER BY time_bucket ASC`, bucketExpr, table, where)

	type sevRow struct {
		TimeBucket string `ch:"time_bucket"`
		Severity   string `ch:"severity"`
		CountU     uint64 `ch:"count_u"`
	}
	var raw []sevRow
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &raw, query, args...); err != nil {
		return nil, err
	}

	// Pivot severity → fields in the caller DTO.
	buckets := make(map[string]*logVolumeRowDTO)
	order := make([]string, 0)
	for _, row := range raw {
		bucket, ok := buckets[row.TimeBucket]
		if !ok {
			bucket = &logVolumeRowDTO{TimeBucket: row.TimeBucket}
			buckets[row.TimeBucket] = bucket
			order = append(order, row.TimeBucket)
		}
		c := int64(row.CountU) //nolint:gosec // domain-bounded
		bucket.Total += c
		switch strings.ToUpper(row.Severity) {
		case "ERROR":
			bucket.Errors += c
		case "WARN", "WARNING":
			bucket.Warnings += c
		case "INFO":
			bucket.Infos += c
		case "DEBUG":
			bucket.Debugs += c
		case "FATAL":
			bucket.Fatals += c
		}
	}
	out := make([]logVolumeRowDTO, 0, len(order))
	for _, tb := range order {
		out = append(out, *buckets[tb])
	}
	return out, nil
}

func (r *logStatsRepository) GetLogStats(ctx context.Context, f shared.LogFilters) ([]facetRowDTO, error) {
	table, _ := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
	where, args := buildRollupLogWhere(f)
	// `scope_name` is not carried by the rollup — dropped. Same applies to
	// `container` / `environment`: they were already filter-only columns
	// on the raw table. Other facets (`severity_text`, `service`, `host`,
	// `pod`) are rollup key columns.
	query := fmt.Sprintf(`
		SELECT 'level' AS dim, severity_text AS value, sumMerge(log_count) AS count_u
		FROM %s WHERE%s GROUP BY severity_text
		UNION ALL
		SELECT 'service_name' AS dim, service AS value, sumMerge(log_count) AS count_u
		FROM %s WHERE%s GROUP BY service
		UNION ALL
		SELECT 'host' AS dim, host AS value, sumMerge(log_count) AS count_u
		FROM %s WHERE%s AND host != '' GROUP BY host
		UNION ALL
		SELECT 'pod' AS dim, pod AS value, sumMerge(log_count) AS count_u
		FROM %s WHERE%s AND pod != '' GROUP BY pod
	`, table, where, table, where, table, where, table, where)

	mergedArgs := append(append(append(args, args...), args...), args...)
	type rowU struct {
		Dim    string `ch:"dim"`
		Value  string `ch:"value"`
		CountU uint64 `ch:"count_u"`
	}
	var raw []rowU
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &raw, query, mergedArgs...); err != nil {
		return nil, fmt.Errorf("logs: stats query: %w", err)
	}
	rows := make([]facetRowDTO, len(raw))
	for i, row := range raw {
		rows[i] = facetRowDTO{
			Dim:   row.Dim,
			Value: row.Value,
			Count: int64(row.CountU), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

// GetLogFields stays on raw — it can target columns not carried by the
// rollup (scope_name, container, environment, attributes_*).
func (r *logStatsRepository) GetLogFields(ctx context.Context, f shared.LogFilters, col string) ([]valueCountRowDTO, error) {
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as value, toInt64(COUNT(*)) as count
		FROM observability.logs WHERE%s AND %s != ''
		GROUP BY %s ORDER BY count DESC LIMIT 200`, col, where, col, col)

	var rows []valueCountRowDTO
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *logStatsRepository) GetTopGroups(ctx context.Context, f shared.LogFilters, query logAggregateQuery) ([]topGroupRowDTO, error) {
	table, _ := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
	groupCol := mapLogGroupCol(query.GroupCol)

	where, args := buildRollupLogWhere(f)

	var topQuery string
	if query.Metric == metricErrorRate {
		topQuery = fmt.Sprintf(`
			SELECT %s AS grp, sumMerge(error_count) AS sort_value_u
			FROM %s
			WHERE%s AND %s != ''
			GROUP BY grp
			ORDER BY sort_value_u DESC
			LIMIT %d
		`, groupCol, table, where, groupCol, query.TopN)
	} else {
		topQuery = fmt.Sprintf(`
			SELECT %s AS grp, sumMerge(log_count) AS sort_value_u
			FROM %s
			WHERE%s AND %s != ''
			GROUP BY grp
			ORDER BY sort_value_u DESC
			LIMIT %d
		`, groupCol, table, where, groupCol, query.TopN)
	}

	type rowU struct {
		GroupValue string `ch:"grp"`
		SortValueU uint64 `ch:"sort_value_u"`
	}
	var raw []rowU
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &raw, topQuery, args...); err != nil {
		return nil, err
	}
	rows := make([]topGroupRowDTO, len(raw))
	for i, row := range raw {
		rows[i] = topGroupRowDTO{
			GroupValue: row.GroupValue,
			SortValue:  int64(row.SortValueU), //nolint:gosec // domain-bounded
		}
	}
	return rows, nil
}

func (r *logStatsRepository) GetAggregateSeries(ctx context.Context, f shared.LogFilters, query logAggregateQuery, groups []string) ([]logAggregateRowDTO, error) {
	if len(groups) == 0 {
		return []logAggregateRowDTO{}, nil
	}

	table, tierStep := rollup.TierTableFor(logsRollupPrefix, f.StartMs, f.EndMs)
	stepMin := resolveStepMinutes(query.Step, tierStep, f.StartMs, f.EndMs)
	bucketExpr := rollupBucketExpr(stepMin)
	groupCol := mapLogGroupCol(query.GroupCol)

	inPlaceholders := strings.Repeat("?,", len(groups))
	inPlaceholders = inPlaceholders[:len(inPlaceholders)-1]
	where, args := buildRollupLogWhere(f)

	combinedArgs := make([]any, 0, len(args)+len(groups))
	combinedArgs = append(combinedArgs, args...)
	combinedArgs = append(combinedArgs, stringSliceToAny(groups)...)

	var sql string
	if query.Metric == metricErrorRate {
		sql = fmt.Sprintf(`
			SELECT %s AS time_bucket,
			       %s AS grp,
			       0 AS cnt_u,
			       if(sumMerge(log_count) = 0, 0,
			          sumMerge(error_count) * 100.0 / sumMerge(log_count)) AS error_rate
			FROM %s
			WHERE%s AND %s IN (%s)
			GROUP BY time_bucket, grp
			ORDER BY time_bucket ASC, grp ASC
		`, bucketExpr, groupCol, table, where, groupCol, inPlaceholders)
	} else {
		sql = fmt.Sprintf(`
			SELECT %s AS time_bucket,
			       %s AS grp,
			       sumMerge(log_count) AS cnt_u,
			       toFloat64(0) AS error_rate
			FROM %s
			WHERE%s AND %s IN (%s)
			GROUP BY time_bucket, grp
			ORDER BY time_bucket ASC, cnt_u DESC
		`, bucketExpr, groupCol, table, where, groupCol, inPlaceholders)
	}

	type rowU struct {
		TimeBucket string  `ch:"time_bucket"`
		GroupValue string  `ch:"grp"`
		CntU       uint64  `ch:"cnt_u"`
		ErrorRate  float64 `ch:"error_rate"`
	}
	var raw []rowU
	if err := r.db.Select(dbutil.ExplorerCtx(ctx), &raw, sql, combinedArgs...); err != nil {
		return nil, err
	}
	rows := make([]logAggregateRowDTO, len(raw))
	for i, row := range raw {
		rows[i] = logAggregateRowDTO{
			TimeBucket: row.TimeBucket,
			GroupValue: row.GroupValue,
			Count:      int64(row.CntU), //nolint:gosec // domain-bounded
			ErrorRate:  row.ErrorRate,
		}
	}
	return rows, nil
}

// mapLogGroupCol translates a raw-table log column name to its rollup
// equivalent. Unknown columns fall through (no translation) — callers that
// need those columns must stay on raw.
func mapLogGroupCol(col string) string {
	switch col {
	case "service_name", "service":
		return "service"
	case "severity_text", "severity":
		return "severity_text"
	case "host":
		return "host"
	case "pod":
		return "pod"
	default:
		return col
	}
}

// resolveStepMinutes converts an explicit step token (e.g. "15m") to
// minutes, clamped to the tier's native step. Falls back to
// queryIntervalMinutes when step is empty or unrecognised.
func resolveStepMinutes(step string, tierStepMin int64, startMs, endMs int64) int64 {
	s := strings.TrimSpace(step)
	if s == "" {
		return queryIntervalMinutes(tierStepMin, startMs, endMs)
	}
	var explicit int64
	switch s {
	case "1m":
		explicit = 1
	case "5m":
		explicit = 5
	case "15m":
		explicit = 15
	case "1h":
		explicit = 60
	case "1d":
		explicit = 1440
	default:
		return queryIntervalMinutes(tierStepMin, startMs, endMs)
	}
	if explicit < tierStepMin {
		return tierStepMin
	}
	return explicit
}

func stringSliceToAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}
