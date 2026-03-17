package analytics

import (
	"context"
	"fmt"
	"strings"

	database "github.com/observability/observability-backend-go/internal/database"
	shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type Repository interface {
	GetLogHistogram(ctx context.Context, f shared.LogFilters, step string) ([]LogHistogramRowDTO, error)
	GetLogVolume(ctx context.Context, f shared.LogFilters, step string) ([]LogVolumeRowDTO, error)
	GetLogStats(ctx context.Context, f shared.LogFilters) ([]FacetRowDTO, error)
	GetLogFields(ctx context.Context, f shared.LogFilters, col string) ([]ValueCountRowDTO, error)
	GetTopGroups(ctx context.Context, f shared.LogFilters, query LogAggregateQuery) ([]TopGroupRowDTO, error)
	GetAggregateSeries(ctx context.Context, f shared.LogFilters, query LogAggregateQuery, groups []string) ([]LogAggregateRowDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetLogHistogram(ctx context.Context, f shared.LogFilters, step string) ([]LogHistogramRowDTO, error) {
	bucketExpr := shared.LogBucketExpr(f.StartMs, f.EndMs)
	if step != "" {
		bucketExpr = shared.LogBucketExprForStep(f.StartMs, f.EndMs, step)
	}
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, severity_text as severity, COUNT(*) as count
		FROM observability.logs WHERE%s
		GROUP BY %s, severity_text
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	var rows []LogHistogramRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLogVolume(ctx context.Context, f shared.LogFilters, step string) ([]LogVolumeRowDTO, error) {
	bucketExpr := shared.LogBucketExpr(f.StartMs, f.EndMs)
	if step != "" {
		bucketExpr = shared.LogBucketExprForStep(f.StartMs, f.EndMs, step)
	}
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       COUNT(*) as total,
		       sum(if(severity_text='ERROR', 1, 0)) as errors,
		       sum(if(severity_text='WARN', 1, 0)) as warnings,
		       sum(if(severity_text='INFO', 1, 0)) as infos,
		       sum(if(severity_text='DEBUG', 1, 0)) as debugs,
		       sum(if(severity_text='FATAL', 1, 0)) as fatals
		FROM observability.logs WHERE%s
		GROUP BY %s
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	var rows []LogVolumeRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLogStats(ctx context.Context, f shared.LogFilters) ([]FacetRowDTO, error) {
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT 'level' AS dim, severity_text AS value, COUNT(*) AS count
		FROM observability.logs WHERE%s GROUP BY severity_text
		UNION ALL
		SELECT 'service_name' AS dim, service AS value, COUNT(*) AS count
		FROM observability.logs WHERE%s GROUP BY service
		UNION ALL
		SELECT 'host' AS dim, host AS value, COUNT(*) AS count
		FROM observability.logs WHERE%s AND host != '' GROUP BY host
		UNION ALL
		SELECT 'pod' AS dim, pod AS value, COUNT(*) AS count
		FROM observability.logs WHERE%s AND pod != '' GROUP BY pod
		UNION ALL
		SELECT 'scope_name' AS dim, scope_name AS value, COUNT(*) AS count
		FROM observability.logs WHERE%s AND scope_name != '' GROUP BY scope_name
	`, where, where, where, where, where)

	mergedArgs := append(append(append(append(args, args...), args...), args...), args...)
	var rows []FacetRowDTO
	if err := r.db.Select(ctx, &rows, query, mergedArgs...); err != nil {
		return nil, fmt.Errorf("logs: stats query: %w", err)
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetLogFields(ctx context.Context, f shared.LogFilters, col string) ([]ValueCountRowDTO, error) {
	where, args := shared.BuildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as value, COUNT(*) as count
		FROM observability.logs WHERE%s AND %s != ''
		GROUP BY %s ORDER BY count DESC LIMIT 200`, col, where, col, col)

	var rows []ValueCountRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopGroups(ctx context.Context, f shared.LogFilters, query LogAggregateQuery) ([]TopGroupRowDTO, error) {
	where, args := shared.BuildLogWhere(f)

	var topQuery string
	if query.Metric == "error_rate" {
		topQuery = fmt.Sprintf(`
			SELECT %s AS grp, sum(if(severity_text IN ('ERROR', 'FATAL'), 1, 0)) AS err_cnt
			FROM observability.logs
			WHERE%s AND %s != ''
			GROUP BY grp
			ORDER BY err_cnt DESC
			LIMIT %d
		`, query.GroupCol, where, query.GroupCol, query.TopN)
	} else {
		topQuery = fmt.Sprintf(`
			SELECT %s AS grp, count() AS cnt
			FROM observability.logs
			WHERE%s AND %s != ''
			GROUP BY grp
			ORDER BY cnt DESC
			LIMIT %d
		`, query.GroupCol, where, query.GroupCol, query.TopN)
	}

	var rows []TopGroupRowDTO
	if err := r.db.Select(ctx, &rows, topQuery, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAggregateSeries(ctx context.Context, f shared.LogFilters, query LogAggregateQuery, groups []string) ([]LogAggregateRowDTO, error) {
	if len(groups) == 0 {
		return []LogAggregateRowDTO{}, nil
	}

	inPlaceholders := strings.Repeat("?,", len(groups))
	inPlaceholders = inPlaceholders[:len(inPlaceholders)-1]
	where, args := shared.BuildLogWhere(f)
	bucketExpr := shared.LogBucketExprForStep(f.StartMs, f.EndMs, query.Step)

	combinedArgs := append(args, stringSliceToAny(groups)...)

	var sql string
	if query.Metric == "error_rate" {
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
			       count() AS cnt,
			       toFloat64(0) AS error_rate
			FROM observability.logs
			WHERE%s AND %s IN (%s)
			GROUP BY time_bucket, grp
			ORDER BY time_bucket ASC, cnt DESC
		`, bucketExpr, query.GroupCol, where, query.GroupCol, inPlaceholders)
	}

	var rows []LogAggregateRowDTO
	if err := r.db.Select(ctx, &rows, sql, combinedArgs...); err != nil {
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
