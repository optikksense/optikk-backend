package explorer

import (
	"context"
	"fmt"
	"strings"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

const metricErrorRate = "error_rate"

type logStatsRepository struct {
	db *dbutil.NativeQuerier
}

func newLogStatsRepository(db *dbutil.NativeQuerier) *logStatsRepository {
	return &logStatsRepository{db: db}
}

func (r *logStatsRepository) GetLogHistogram(ctx context.Context, f shared.LogFilters, step string) ([]logHistogramRowDTO, error) {
	bucketExpr := shared.LogBucketExpr(f.StartMs, f.EndMs)
	if step != "" {
		bucketExpr = shared.LogBucketExprForStep(f.StartMs, f.EndMs, step)
	}
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, severity_text as severity, toInt64(COUNT(*)) as count
		FROM observability.logs WHERE%s
		GROUP BY %s, severity_text
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	var rows []logHistogramRowDTO
	if err := r.db.SelectExplorer(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *logStatsRepository) GetLogVolume(ctx context.Context, f shared.LogFilters, step string) ([]logVolumeRowDTO, error) {
	bucketExpr := shared.LogBucketExpr(f.StartMs, f.EndMs)
	if step != "" {
		bucketExpr = shared.LogBucketExprForStep(f.StartMs, f.EndMs, step)
	}
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       toInt64(COUNT(*)) as total,
		       toInt64(sum(if(severity_text='ERROR', 1, 0))) as errors,
		       toInt64(sum(if(severity_text='WARN', 1, 0))) as warnings,
		       toInt64(sum(if(severity_text='INFO', 1, 0))) as infos,
		       toInt64(sum(if(severity_text='DEBUG', 1, 0))) as debugs,
		       toInt64(sum(if(severity_text='FATAL', 1, 0))) as fatals
		FROM observability.logs WHERE%s
		GROUP BY %s
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	var rows []logVolumeRowDTO
	if err := r.db.SelectExplorer(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *logStatsRepository) GetLogStats(ctx context.Context, f shared.LogFilters) ([]facetRowDTO, error) {
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT 'level' AS dim, severity_text AS value, toInt64(COUNT(*)) AS count
		FROM observability.logs WHERE%s GROUP BY severity_text
		UNION ALL
		SELECT 'service_name' AS dim, service AS value, toInt64(COUNT(*)) AS count
		FROM observability.logs WHERE%s GROUP BY service
		UNION ALL
		SELECT 'host' AS dim, host AS value, toInt64(COUNT(*)) AS count
		FROM observability.logs WHERE%s AND host != '' GROUP BY host
		UNION ALL
		SELECT 'pod' AS dim, pod AS value, toInt64(COUNT(*)) AS count
		FROM observability.logs WHERE%s AND pod != '' GROUP BY pod
		UNION ALL
		SELECT 'scope_name' AS dim, scope_name AS value, toInt64(COUNT(*)) AS count
		FROM observability.logs WHERE%s AND scope_name != '' GROUP BY scope_name
	`, where, where, where, where, where)

	mergedArgs := append(append(append(append(args, args...), args...), args...), args...)
	var rows []facetRowDTO
	if err := r.db.SelectExplorer(ctx, &rows, query, mergedArgs...); err != nil {
		return nil, fmt.Errorf("logs: stats query: %w", err)
	}
	return rows, nil
}

func (r *logStatsRepository) GetLogFields(ctx context.Context, f shared.LogFilters, col string) ([]valueCountRowDTO, error) {
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as value, toInt64(COUNT(*)) as count
		FROM observability.logs WHERE%s AND %s != ''
		GROUP BY %s ORDER BY count DESC LIMIT 200`, col, where, col, col)

	var rows []valueCountRowDTO
	if err := r.db.SelectExplorer(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *logStatsRepository) GetTopGroups(ctx context.Context, f shared.LogFilters, query logAggregateQuery) ([]topGroupRowDTO, error) {
	where, args := shared.BuildLogWhere(f)

	var topQuery string
	if query.Metric == metricErrorRate {
		topQuery = fmt.Sprintf(`
			SELECT %s AS grp, toInt64(sum(if(severity_text IN ('ERROR', 'FATAL'), 1, 0))) AS sort_value
			FROM observability.logs
			WHERE%s AND %s != ''
			GROUP BY grp
			ORDER BY sort_value DESC
			LIMIT %d
		`, query.GroupCol, where, query.GroupCol, query.TopN)
	} else {
		topQuery = fmt.Sprintf(`
			SELECT %s AS grp, toInt64(count()) AS sort_value
			FROM observability.logs
			WHERE%s AND %s != ''
			GROUP BY grp
			ORDER BY sort_value DESC
			LIMIT %d
		`, query.GroupCol, where, query.GroupCol, query.TopN)
	}

	var rows []topGroupRowDTO
	if err := r.db.SelectExplorer(ctx, &rows, topQuery, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *logStatsRepository) GetAggregateSeries(ctx context.Context, f shared.LogFilters, query logAggregateQuery, groups []string) ([]logAggregateRowDTO, error) {
	if len(groups) == 0 {
		return []logAggregateRowDTO{}, nil
	}

	inPlaceholders := strings.Repeat("?,", len(groups))
	inPlaceholders = inPlaceholders[:len(inPlaceholders)-1]
	where, args := shared.BuildLogWhere(f)
	bucketExpr := shared.LogBucketExprForStep(f.StartMs, f.EndMs, query.Step)

	combinedArgs := make([]any, 0, len(args)+len(groups))
	combinedArgs = append(combinedArgs, args...)
	combinedArgs = append(combinedArgs, stringSliceToAny(groups)...)

	var sql string
	if query.Metric == metricErrorRate {
		sql = fmt.Sprintf(`
			SELECT %s AS time_bucket,
			       %s AS grp,
			       toInt64(0) AS cnt,
			       if(count() > 0,
			          sum(if(severity_text IN ('ERROR', 'FATAL'), 1, 0)) * 100.0 / count(),
			          0) AS error_rate
			FROM observability.logs
			WHERE%s AND %s IN (%s)
			GROUP BY time_bucket, grp
			ORDER BY time_bucket ASC, grp ASC
		`, bucketExpr, query.GroupCol, where, query.GroupCol, inPlaceholders)
	} else {
		sql = fmt.Sprintf(`
			SELECT %s AS time_bucket,
			       %s AS grp,
			       toInt64(count()) AS cnt,
			       toFloat64(0) AS error_rate
			FROM observability.logs
			WHERE%s AND %s IN (%s)
			GROUP BY time_bucket, grp
			ORDER BY time_bucket ASC, cnt DESC
		`, bucketExpr, query.GroupCol, where, query.GroupCol, inPlaceholders)
	}

	var rows []logAggregateRowDTO
	if err := r.db.SelectExplorer(ctx, &rows, sql, combinedArgs...); err != nil {
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
