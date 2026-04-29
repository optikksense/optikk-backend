package explorer

import (
	"context"
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
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
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

func (r *ClickHouseRepository) ListTagKeys(ctx context.Context, teamID, startMs, endMs int64, metricName string) ([]TagKeyResult, error) {
	const query = `
		SELECT DISTINCT tag_key FROM (
			SELECT DISTINCT arrayJoin(mapKeys(JSONAllPathsWithTypes(attributes))) AS tag_key
			FROM observability.metrics_1m
			PREWHERE team_id        = @teamID
			     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
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
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
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
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name = @metricName` + resourceWhere + `
		)
		SELECT ` + selectCols + `,
		       sum(val_sum)   AS val_sum,
		       sum(val_count) AS val_count,
		       min(val_min)   AS val_min,
		       max(val_max)   AS val_max
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
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
	bucketSeconds := float64(bucketDurationSeconds(f.StartMs, f.EndMs, f.Step))
	out := make([]TimeseriesPoint, len(rows))
	for i, row := range rows {
		var val float64
		switch f.Aggregation {
		case "sum":
			val = row.Sum
		case "avg":
			if row.Count > 0 {
				val = row.Sum / float64(row.Count)
			}
		case "min":
			val = row.Min
		case "max":
			val = row.Max
		case "count":
			val = float64(row.Count)
		case "rate":
			val = row.Sum / bucketSeconds
		default:
			if row.Count > 0 {
				val = row.Sum / float64(row.Count)
			}
		}
		out[i] = TimeseriesPoint{Timestamp: row.Timestamp, Value: val}
	}
	return out, nil
}

func metricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	bucketStart := timebucket.BucketStart(startMs / 1000)
	bucketEnd := timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
	return bucketStart, bucketEnd
}

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
