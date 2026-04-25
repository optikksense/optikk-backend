package metrics

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const tableMetrics = "observability.metrics"

// materializedDimensions maps user-facing tag key names to ClickHouse materialized
// column names. These columns are extracted from attributes at insert time.
var materializedDimensions = map[string]string{
	"service":		"service",
	"host":			"host",
	"environment":		"environment",
	"k8s_namespace":	"k8s_namespace",
	"http_method":		"http_method",
	"http_status_code":	"toString(http_status_code)",
}

// allowedAggregations is the set of aggregation functions we support.
var allowedAggregations = map[string]bool{
	"avg":	true, "sum": true, "min": true, "max": true, "count": true,
	"p50":	true, "p75": true, "p95": true, "p99": true,
	"rate":	true,
}

// allowedOperators is the set of filter operators we support.
var allowedOperators = map[string]bool{
	"=":	true, "!=": true, "IN": true, "NOT IN": true,
}

type Repository interface {
	ListMetricNames(ctx context.Context, teamID int64, startMs, endMs int64, search string) ([]MetricNameResult, error)
	ListTagKeys(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]TagKeyResult, error)
	ListTagValues(ctx context.Context, teamID int64, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error)
	QueryTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, query MetricQuery, step string) ([]TimeseriesPoint, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) ListMetricNames(ctx context.Context, teamID int64, startMs, endMs int64, search string) ([]MetricNameResult, error) {
	query := fmt.Sprintf(`
		SELECT
			metric_name,
			any(metric_type) AS metric_type,
			any(unit)        AS unit,
			any(description) AS description
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  AND metric_name ILIKE @search
		GROUP BY metric_name
		ORDER BY metric_name
		LIMIT 100
	`, tableMetrics)

	params := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("search", "%"+search+"%"),
	)

	var rows []metricNameDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListMetricNames", &rows, query, params...); err != nil {
		return nil, err
	}
	results := make([]MetricNameResult, len(rows))
	for i, r := range rows {
		results[i] = MetricNameResult{
			MetricName:	r.MetricName,
			MetricType:	r.MetricType,
			Unit:		r.Unit,
			Description:	r.Description,
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) ListTagKeys(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]TagKeyResult, error) {
	// For native JSON columns, use mapKeys(JSONAllPathsWithTypes()) to extract
	// dynamic attribute key names instead of JSONExtractKeys (which is for String JSON).
	query := fmt.Sprintf(`
		SELECT DISTINCT tag_key FROM (
			SELECT DISTINCT arrayJoin(mapKeys(JSONAllPathsWithTypes(attributes))) AS tag_key
			FROM %s
			WHERE team_id = @teamID
			  AND timestamp BETWEEN @start AND @end
			  AND metric_name = @metricName
			UNION ALL
			SELECT * FROM (
				SELECT 'service' AS tag_key
				UNION ALL SELECT 'host'
				UNION ALL SELECT 'environment'
				UNION ALL SELECT 'k8s_namespace'
				UNION ALL SELECT 'http_method'
				UNION ALL SELECT 'http_status_code'
			)
		)
		ORDER BY tag_key
		LIMIT 200
	`, tableMetrics)

	params := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", metricName),
	)

	var rows []tagKeyDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListTagKeys", &rows, query, params...); err != nil {
		return nil, err
	}
	results := make([]TagKeyResult, len(rows))
	for i, r := range rows {
		results[i] = TagKeyResult{TagKey: r.TagKey}
	}
	return results, nil
}

func (r *ClickHouseRepository) ListTagValues(ctx context.Context, teamID int64, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error) {
	var valueExpr string
	if col, ok := materializedDimensions[tagKey]; ok {
		valueExpr = col
	} else {
		valueExpr = fmt.Sprintf("attributes.`%s`::String", sanitizeAttrKey(tagKey))
	}

	query := fmt.Sprintf(`
		SELECT
			%s AS tag_value,
			count() AS count
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND %s != ''
		GROUP BY tag_value
		ORDER BY count DESC
		LIMIT 100
	`, valueExpr, tableMetrics, valueExpr)

	params := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", metricName),
	)

	var rows []tagValueDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListTagValues", &rows, query, params...); err != nil {
		return nil, err
	}
	results := make([]TagValueResult, len(rows))
	for i, r := range rows {
		results[i] = TagValueResult{TagValue: r.TagValue, Count: r.Count}
	}
	return results, nil
}

// QueryTimeseries executes a time-series query. If step is non-empty, it is used
// for time bucketing; otherwise adaptive bucketing is applied based on the time range.
func (r *ClickHouseRepository) QueryTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, query MetricQuery, step string) ([]TimeseriesPoint, error) {
	if !allowedAggregations[query.Aggregation] {
		return nil, fmt.Errorf("unsupported aggregation: %s", query.Aggregation)
	}

	var bucket string
	if step != "" {
		bucket = timebucket.ByName(step).GetBucketExpression()
	} else {
		bucket = timebucket.Expression(startMs, endMs)
	}

	aggExpr := buildAggExpr(query.Aggregation, startMs, endMs, step)

	// Build GROUP BY columns
	groupByCols := []string{"time_bucket"}
	selectCols := []string{
		fmt.Sprintf("%s AS time_bucket", bucket),
	}
	for _, key := range query.GroupBy {
		colExpr := resolveColumn(key)
		selectCols = append(selectCols, fmt.Sprintf("%s AS `group_%s`", colExpr, sanitizeAttrKey(key)))
		groupByCols = append(groupByCols, fmt.Sprintf("`group_%s`", sanitizeAttrKey(key)))
	}
	selectCols = append(selectCols, fmt.Sprintf("%s AS agg_value", aggExpr))

	// Build WHERE filter clauses
	params := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", query.MetricName),
	)
	filterClauses, filterParams := buildFilterClauses(query.Filters)
	params = append(params, filterParams...)

	whereClause := `team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  AND metric_name = @metricName`
	if len(filterClauses) > 0 {
		whereClause += "\n		  AND " + strings.Join(filterClauses, "\n		  AND ")
	}

	sql := fmt.Sprintf(`
		SELECT %s
		FROM %s
		WHERE %s
		GROUP BY %s
		ORDER BY time_bucket ASC
		LIMIT 10000
		SETTINGS max_execution_time = 30
	`,
		strings.Join(selectCols, ", "),
		tableMetrics,
		whereClause,
		strings.Join(groupByCols, ", "),
	)

	var rows []timeseriesPointDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "metrics.QueryTimeseries", &rows, sql, params...); err != nil {
		return nil, err
	}
	results := make([]TimeseriesPoint, len(rows))
	for i, r := range rows {
		results[i] = TimeseriesPoint{Timestamp: r.Timestamp, Value: r.Value}
	}
	return results, nil
}

// buildAggExpr returns the ClickHouse aggregation expression.
func buildAggExpr(agg string, startMs, endMs int64, step string) string {
	switch agg {
	case "avg":
		return "avg(if(metric_type = 'Histogram', hist_sum / nullIf(hist_count, 0), value))"
	case "sum":
		return "sum(if(metric_type = 'Histogram', hist_sum, value))"
	case "min":
		return "min(if(metric_type = 'Histogram', hist_sum / nullIf(hist_count, 0), value))"
	case "max":
		return "max(if(metric_type = 'Histogram', hist_sum / nullIf(hist_count, 0), value))"
	case "count":
		return "sum(if(metric_type = 'Histogram', hist_count, 1))"
	case "rate":
		bucketSeconds := bucketDurationSeconds(startMs, endMs, step)
		return fmt.Sprintf("sum(if(metric_type = 'Histogram', hist_count, value)) / %d", bucketSeconds)
	case "p50":
		return "quantileTDigestWeighted(0.50)(hist_sum / nullIf(hist_count, 0), toUInt64(hist_count))"
	case "p75":
		return "quantileTDigestWeighted(0.75)(hist_sum / nullIf(hist_count, 0), toUInt64(hist_count))"
	case "p95":
		return "quantileTDigestWeighted(0.95)(hist_sum / nullIf(hist_count, 0), toUInt64(hist_count))"
	case "p99":
		return "quantileTDigestWeighted(0.99)(hist_sum / nullIf(hist_count, 0), toUInt64(hist_count))"
	default:
		return "avg(value)"
	}
}

// bucketDurationSeconds returns the duration in seconds for a given step or adaptive bucket.
func bucketDurationSeconds(startMs, endMs int64, step string) int64 {
	switch step {
	case "1m":
		return 60
	case "5m":
		return 300
	case "15m":
		return 900
	case "1h":
		return 3600
	case "1d":
		return 86400
	default:
		// Adaptive: derive from the time range
		h := (endMs - startMs) / 3_600_000
		switch {
		case h <= 3:
			return 60
		case h <= 24:
			return 300
		case h <= 168:
			return 3600
		default:
			return 86400
		}
	}
}

// resolveColumn maps a tag key to its ClickHouse column expression.
func resolveColumn(key string) string {
	if col, ok := materializedDimensions[key]; ok {
		return col
	}
	return fmt.Sprintf("attributes.`%s`::String", sanitizeAttrKey(key))
}

// buildFilterClauses converts TagFilter slices to SQL WHERE clauses and params.
func buildFilterClauses(filters []TagFilter) ([]string, []any) {
	var clauses []string
	var params []any

	for i, f := range filters {
		if !allowedOperators[f.Operator] || len(f.Values) == 0 {
			continue
		}

		colExpr := resolveColumn(f.Key)
		paramName := fmt.Sprintf("fv%d", i)

		switch f.Operator {
		case "=":
			clauses = append(clauses, fmt.Sprintf("%s = @%s", colExpr, paramName))
			params = append(params, clickhouse.Named(paramName, f.Values[0]))
		case "!=":
			clauses = append(clauses, fmt.Sprintf("%s != @%s", colExpr, paramName))
			params = append(params, clickhouse.Named(paramName, f.Values[0]))
		case "IN":
			clauses = append(clauses, fmt.Sprintf("%s IN (@%s)", colExpr, paramName))
			params = append(params, clickhouse.Named(paramName, f.Values))
		case "NOT IN":
			clauses = append(clauses, fmt.Sprintf("%s NOT IN (@%s)", colExpr, paramName))
			params = append(params, clickhouse.Named(paramName, f.Values))
		}
	}
	return clauses, params
}

// sanitizeAttrKey strips characters that could be used for SQL injection in attribute key names.
func sanitizeAttrKey(key string) string {
	var b strings.Builder
	for _, r := range key {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
