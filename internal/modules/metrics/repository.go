package metrics

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/metrics/shared/resource"
)

const tableMetrics = "observability.metrics"

// dataPointDimensions maps non-resource user-facing tag keys to their JSON-path
// extraction against the per-point `attributes` column. Resource keys are not
// listed here — they go through the resource package's Canonical/ColumnExpr.
var dataPointDimensions = map[string]string{
	"http_method":      "attributes.`http.method`::String",
	"http.method":      "attributes.`http.method`::String",
	"http_status_code": "attributes.`http.status_code`::String",
	"http.status_code": "attributes.`http.status_code`::String",
}

// allowedAggregations is the set of aggregation functions we support.
var allowedAggregations = map[string]bool{
	"avg": true, "sum": true, "min": true, "max": true, "count": true,
	"p50": true, "p75": true, "p95": true, "p99": true,
	"rate": true,
}

// allowedOperators is the set of filter operators we support.
var allowedOperators = map[string]bool{
	"=": true, "!=": true, "IN": true, "NOT IN": true,
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
			MetricName:  r.MetricName,
			MetricType:  r.MetricType,
			Unit:        r.Unit,
			Description: r.Description,
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
	if canonical := resource.Canonical(tagKey); canonical != "" {
		return r.listResourceTagValues(ctx, teamID, startMs, endMs, metricName, canonical)
	}

	var valueExpr string
	if expr, ok := dataPointDimensions[tagKey]; ok {
		valueExpr = expr
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

// listResourceTagValues runs against the metrics_resource dictionary instead
// of raw observability.metrics. The dictionary is narrow (LowCardinality
// columns) and keyed per (metric_name, fingerprint), so we scope by
// metric_name to avoid leaking values from other metrics.
func (r *ClickHouseRepository) listResourceTagValues(ctx context.Context, teamID int64, startMs, endMs int64, metricName, canonicalKey string) ([]TagValueResult, error) {
	bucketStart, bucketEnd := resource.BucketBounds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
			%[1]s AS tag_value,
			count() AS count
		FROM %[2]s
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE metric_name = @metricName
		  AND %[1]s != ''
		GROUP BY tag_value
		ORDER BY count DESC
		LIMIT 100
	`, canonicalKey, resource.Table)

	params := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}

	var rows []tagValueDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListResourceTagValues", &rows, query, params...); err != nil {
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
//
// Resource-attribute filters (service / host / environment / k8s_namespace) are
// pre-resolved into a fingerprint set against metrics_resource_1h and applied as
// `fingerprint IN (...)`. Non-resource filters continue to extract
// from the per-point `attributes` JSON inline.
func (r *ClickHouseRepository) QueryTimeseries(ctx context.Context, teamID int64, startMs, endMs int64, query MetricQuery, step string) ([]TimeseriesPoint, error) {
	if !allowedAggregations[query.Aggregation] {
		return nil, fmt.Errorf("unsupported aggregation: %s", query.Aggregation)
	}

	// Bucket grain comes from the metrics tier table the resolver picked
	// (1h / 6h / 1d). Reader groups by the stored ts_bucket directly — no
	// CH-side bucket math, see internal/infra/timebucket.
	_, _, _ = step, startMs, endMs
	bucket := "toString(ts_bucket)"

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

	// Split resource-scoped filters out of the tag-filter list so they can be
	// resolved into a fingerprint set instead of evaluated row-by-row on raw.
	resourceFilters, nonResourceFilters := partitionResourceFilters(teamID, startMs, endMs, query.Filters)

	params := append(dbutil.SimpleBaseParams(teamID, startMs, endMs),
		clickhouse.Named("metricName", query.MetricName),
	)
	filterClauses, filterParams := buildFilterClauses(nonResourceFilters)
	params = append(params, filterParams...)

	whereClause := `team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  AND metric_name = @metricName`
	if len(filterClauses) > 0 {
		whereClause += "\n		  AND " + strings.Join(filterClauses, "\n		  AND ")
	}

	whereClause, params, empty, err := resource.WithFingerprints(ctx, r.db, resourceFilters, whereClause, params)
	if err != nil {
		return nil, err
	}
	if empty {
		return []TimeseriesPoint{}, nil
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

// partitionResourceFilters splits a TagFilter slice into resource-scoped
// filters (routed through the fingerprint resolver) and non-resource filters
// (evaluated inline against the `attributes` JSON column).
func partitionResourceFilters(teamID, startMs, endMs int64, filters []TagFilter) (resource.Filters, []TagFilter) {
	rf := resource.Filters{TeamID: teamID, StartMs: startMs, EndMs: endMs}
	var rest []TagFilter
	for _, f := range filters {
		if !allowedOperators[f.Operator] || len(f.Values) == 0 {
			continue
		}
		canonical := resource.Canonical(f.Key)
		if canonical == "" {
			rest = append(rest, f)
			continue
		}
		negated := f.Operator == "!=" || f.Operator == "NOT IN"
		switch canonical {
		case "service":
			if negated {
				rf.ExcludeServices = append(rf.ExcludeServices, f.Values...)
			} else {
				rf.Services = append(rf.Services, f.Values...)
			}
		case "host":
			if negated {
				rf.ExcludeHosts = append(rf.ExcludeHosts, f.Values...)
			} else {
				rf.Hosts = append(rf.Hosts, f.Values...)
			}
		case "environment":
			if negated {
				rf.ExcludeEnvironments = append(rf.ExcludeEnvironments, f.Values...)
			} else {
				rf.Environments = append(rf.Environments, f.Values...)
			}
		case "k8s_namespace":
			if negated {
				rf.ExcludeK8sNamespaces = append(rf.ExcludeK8sNamespaces, f.Values...)
			} else {
				rf.K8sNamespaces = append(rf.K8sNamespaces, f.Values...)
			}
		}
	}
	return rf, rest
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

// resolveColumn maps a tag key to its ClickHouse column expression against
// raw observability.metrics. Resource keys come from the `resource` JSON
// column; data-point dimensions come from the `attributes` JSON column.
func resolveColumn(key string) string {
	if canonical := resource.Canonical(key); canonical != "" {
		return resource.ColumnExpr(canonical)
	}
	if expr, ok := dataPointDimensions[key]; ok {
		return expr
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
