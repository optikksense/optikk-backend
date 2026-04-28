package explorer

import (
	"context"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/metrics/filter"
)

type Repository interface {
	ListMetricNames(ctx context.Context, teamID, startMs, endMs int64, search string) ([]MetricNameResult, error)
	ListTagKeys(ctx context.Context, teamID, startMs, endMs int64, metricName string) ([]TagKeyResult, error)
	ListTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error)
	QueryTimeseries(ctx context.Context, f filter.Filters) ([]TimeseriesPoint, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// ListMetricNames runs in two phases via a single CH statement: a CTE
// narrows the candidate metric_name set against observability.metrics_resource
// (LowCardinality dictionary, hour-bucketed) — that scan is tiny — then the
// outer SELECT picks up metric_type / unit / description from raw metrics
// PREWHEREd on (team_id, ts_bucket_hour, metric_name IN names).
func (r *ClickHouseRepository) ListMetricNames(ctx context.Context, teamID, startMs, endMs int64, search string) ([]MetricNameResult, error) {
	const query = `
		WITH names AS (
		    SELECT DISTINCT metric_name
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		    WHERE metric_name ILIKE @search
		)
		SELECT metric_name,
		       any(metric_type) AS metric_type,
		       any(unit)        AS unit,
		       any(description) AS description
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name    IN names
		GROUP BY metric_name
		ORDER BY metric_name
		LIMIT 100`
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("search", "%"+search+"%"),
	}
	var rows []metricNameDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListMetricNames", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]MetricNameResult, len(rows))
	for i, row := range rows {
		out[i] = MetricNameResult{
			MetricName:  row.MetricName,
			MetricType:  row.MetricType,
			Unit:        row.Unit,
			Description: row.Description,
		}
	}
	return out, nil
}

// ListTagKeys returns the set of tag keys present on data points of the
// given metric. The dynamic-keys side scans raw metrics (the only place
// the per-data-point attributes JSON column lives) PREWHERE'd on the first
// three PK slots; UNION ALL adds the canonical resource keys so the
// frontend always sees them in the autocomplete list.
func (r *ClickHouseRepository) ListTagKeys(ctx context.Context, teamID, startMs, endMs int64, metricName string) ([]TagKeyResult, error) {
	const query = `
		SELECT DISTINCT tag_key FROM (
			SELECT DISTINCT arrayJoin(mapKeys(JSONAllPathsWithTypes(attributes))) AS tag_key
			FROM observability.metrics
			PREWHERE team_id        = @teamID
			     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
			     AND metric_name    = @metricName
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
		LIMIT 200`
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}
	var rows []tagKeyDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListTagKeys", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]TagKeyResult, len(rows))
	for i, row := range rows {
		out[i] = TagKeyResult{TagKey: row.TagKey}
	}
	return out, nil
}

// ListTagValues returns the top-N values for a tag key on a given metric.
// Resource keys go through metrics_resource (narrow LowCardinality columns,
// hour-bucketed); arbitrary tag keys read the per-data-point attributes
// JSON column on raw observability.metrics.
func (r *ClickHouseRepository) ListTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error) {
	if canonical := filter.Canonical(tagKey); canonical != "" {
		return r.listResourceTagValues(ctx, teamID, startMs, endMs, metricName, canonical)
	}
	return r.listAttributeTagValues(ctx, teamID, startMs, endMs, metricName, tagKey)
}

func (r *ClickHouseRepository) listResourceTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, canonical string) ([]TagValueResult, error) {
	col := filter.ResourceColumn(canonical)
	if col == "" {
		return nil, nil
	}
	query := `
		SELECT ` + col + ` AS tag_value,
		       count()    AS count
		FROM observability.metrics_resource
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		WHERE ` + col + ` != ''
		GROUP BY tag_value
		ORDER BY count DESC
		LIMIT 100`
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}
	var rows []tagValueDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListResourceTagValues", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]TagValueResult, len(rows))
	for i, row := range rows {
		out[i] = TagValueResult{TagValue: row.TagValue, Count: row.Count}
	}
	return out, nil
}

func (r *ClickHouseRepository) listAttributeTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]TagValueResult, error) {
	col := filter.AttrColumn(tagKey)
	query := `
		SELECT ` + col + ` AS tag_value,
		       count()      AS count
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name    = @metricName
		WHERE ` + col + ` != ''
		GROUP BY tag_value
		ORDER BY count DESC
		LIMIT 100`
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}
	var rows []tagValueDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListAttributeTagValues", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]TagValueResult, len(rows))
	for i, row := range rows {
		out[i] = TagValueResult{TagValue: row.TagValue, Count: row.Count}
	}
	return out, nil
}

// QueryTimeseries runs the explorer's main aggregation against
// observability.metrics with an inline metrics_resource CTE for resource
// filters. One round-trip per call regardless of filter shape.
func (r *ClickHouseRepository) QueryTimeseries(ctx context.Context, f filter.Filters) ([]TimeseriesPoint, error) {
	resourceWhere, where, filterArgs := filter.BuildClauses(f)

	selectCols := "toString(ts_bucket) AS time_bucket"
	groupByCols := "time_bucket"
	for _, key := range f.GroupBy {
		col := filter.GroupByColumn(key)
		alias := "`group_" + filter.SanitizeKey(key) + "`"
		selectCols += ", " + col + " AS " + alias
		groupByCols += ", " + alias
	}
	selectCols += ", " + buildAggExpr(f.Aggregation, f.StartMs, f.EndMs, f.Step) + " AS agg_value"

	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name = @metricName` + resourceWhere + `
		)
		SELECT ` + selectCols + `
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint    IN active_fps
		     AND metric_name    = @metricName
		WHERE timestamp BETWEEN @start AND @end` + where + `
		GROUP BY ` + groupByCols + `
		ORDER BY time_bucket ASC
		LIMIT 10000
		SETTINGS max_execution_time = 30`

	args := append(metricArgs(f), filterArgs...)
	var rows []timeseriesPointDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "metrics.QueryTimeseries", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]TimeseriesPoint, len(rows))
	for i, row := range rows {
		out[i] = TimeseriesPoint{Timestamp: row.Timestamp, Value: row.Value}
	}
	return out, nil
}

// metricBucketBounds returns the hour-aligned [bucketStart, bucketEnd)
// range covering [startMs, endMs] in metrics_resource / metrics PK terms.
// Same shape as the per-module helpers in infrastructure/{cpu,memory,...}.
func metricBucketBounds(startMs, endMs int64) (time.Time, time.Time) {
	bucketStart := timebucket.MetricsHourBucket(startMs / 1000)
	bucketEnd := timebucket.MetricsHourBucket(endMs / 1000).Add(time.Hour)
	return bucketStart, bucketEnd
}

// metricArgs binds the 6 parameters QueryTimeseries needs: team scope,
// hour-aligned ts_bucket bounds, metric name, and the row-side timestamp
// range.
func metricArgs(f filter.Filters) []any {
	bucketStart, bucketEnd := metricBucketBounds(f.StartMs, f.EndMs)
	return []any{
		clickhouse.Named("teamID", uint32(f.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", f.MetricName),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}
}

// buildAggExpr returns the ClickHouse aggregation expression for an
// aggregation name. The rate branch divides by a server-computed bucket
// duration (not user input) so string-concat is safe.
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
		return "sum(if(metric_type = 'Histogram', hist_count, value)) / " + strconv.FormatInt(bucketSeconds, 10)
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

// bucketDurationSeconds returns the duration in seconds for a given step
// or, when step is empty, an adaptive grain based on the time range.
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
