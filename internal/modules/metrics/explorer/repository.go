package explorer

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/metrics/filter"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ListMetricNames(ctx context.Context, teamID, startMs, endMs int64, search string) ([]metricNameDTO, error) {
	// Active metric names come from rollups and metrics_meta metadata.
	const query = `
		SELECT mn.metric_name      AS metric_name,
		       any(md.metric_type) AS metric_type,
		       any(md.unit)        AS unit,
		       any(md.description) AS description
		FROM (
		    SELECT DISTINCT metric_name FROM observability.metrics_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		    UNION DISTINCT
		    SELECT DISTINCT metric_name FROM observability.metrics_hist_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		) mn
		LEFT JOIN observability.metrics_meta md
		    ON md.team_id = @teamID AND md.metric_name = mn.metric_name
		WHERE mn.metric_name ILIKE @search
		GROUP BY metric_name
		ORDER BY metric_name
		LIMIT 100`
	bucketStart, bucketEnd := chargs.BucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("search", "%"+search+"%"),
	}
	var rows []metricNameDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListMetricNames", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *Repository) ListAttributeTagKeys(ctx context.Context, teamID, startMs, endMs int64, metricName string) ([]tagKeyDTO, error) {
	bucketStart, bucketEnd := chargs.BucketBounds(startMs, endMs)

	// Reads distinct attribute keys from metrics_attr (metric_name leads PK).
	const dynamicQuery = `
		SELECT DISTINCT arrayJoin(mapKeys(JSONAllPathsWithTypes(attributes))) AS tag_key
		FROM observability.metrics_attr
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		ORDER BY tag_key
		LIMIT 200`

	dynamicArgs := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}

	var rows []tagKeyDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListTagKeys.GetDynamicKeys", &rows, dynamicQuery, dynamicArgs...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *Repository) ListResourceTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, canonical string) ([]tagValueDTO, error) {
	col := filter.ResourceColumn(canonical)
	if col == "" {
		return nil, nil
	}
	query := `
		WITH fps AS (
		    SELECT DISTINCT fingerprint FROM observability.metrics_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		    UNION DISTINCT
		    SELECT DISTINCT fingerprint FROM observability.metrics_hist_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)
		SELECT ` + col + ` AS tag_value,
		       count()    AS count
		FROM observability.metrics_resource
		PREWHERE team_id   = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE fingerprint IN fps
		  AND ` + col + ` != ''
		GROUP BY tag_value
		ORDER BY count DESC
		LIMIT 100`
	bucketStart, bucketEnd := chargs.BucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}
	var rows []tagValueDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListResourceTagValues", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *Repository) ListAttributeTagValues(ctx context.Context, teamID, startMs, endMs int64, metricName, tagKey string) ([]tagValueDTO, error) {
	bucketStart, bucketEnd := chargs.BucketBounds(startMs, endMs)

	col := filter.AttrColumn(tagKey)
	query := `
		SELECT ` + col + ` AS tag_value,
		       count()      AS count
		FROM observability.metrics_attr
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		WHERE ` + col + ` != ''
		GROUP BY tag_value
		ORDER BY count DESC
		LIMIT 100`

	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}
	var rows []tagValueDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListAttributeTagValues", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// ListTagValuesForKeys returns distinct values and counts for tag keys.
func (r *Repository) ListTagValuesForKeys(ctx context.Context, teamID, startMs, endMs int64, metricName string, keys []string) ([]tagKeyValueDTO, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	arms, needFps, armArgs := filter.BuildTagValueArms(keys)
	if len(arms) == 0 {
		return nil, nil
	}
	bucketStart, bucketEnd := chargs.BucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", metricName),
	}
	args = append(args, armArgs...)

	var fpsCTE string
	if needFps {
		fpsCTE = `
		WITH fps AS (
		    SELECT DISTINCT fingerprint FROM observability.metrics_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		    UNION DISTINCT
		    SELECT DISTINCT fingerprint FROM observability.metrics_hist_1m
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)`
	}
	query := fpsCTE + `
		SELECT tag_key, tag_value, sum(c) AS count
		FROM (` + strings.Join(arms, "\n\t\t\tUNION ALL") + `
		)
		GROUP BY tag_key, tag_value
		ORDER BY tag_key, count DESC
		LIMIT 100 BY tag_key`

	var rows []tagKeyValueDTO
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "metrics.ListTagValuesForKeys", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// QueryRollupSeries queries aggregated metrics (scalar or raw) for timeseries.
func (r *Repository) QueryRollupSeries(ctx context.Context, f filter.Filters) ([]timeseriesPointDTO, error) {
	fromTable, cte, joins, selectCols, groupByCols, filterArgs := filter.BuildSelection(f)

	var valSelect string
	if fromTable == "observability.metrics" {
		valSelect = `
		       sum(value)     AS val_sum,
		       count()        AS val_count,
		       min(value)     AS val_min,
		       max(value)     AS val_max`
	} else {
		valSelect = `
		       sum(val_sum)   AS val_sum,
		       sum(val_count) AS val_count,
		       min(val_min)   AS val_min,
		       max(val_max)   AS val_max`
	}

	query := cte + `
		SELECT ` + selectCols + `,` + valSelect + `
		FROM ` + fromTable + ` AS m` + joins + `
		PREWHERE m.team_id     = @teamID
		     AND m.ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND m.metric_name = @metricName
		     AND m.timestamp   BETWEEN @start AND @end
		GROUP BY ` + groupByCols + `
		ORDER BY bucket_at ASC
		LIMIT 10000
		SETTINGS max_execution_time = 30`

	args := append(metricArgs(f), filterArgs...)
	var rows []timeseriesPointDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "metrics.QueryRollupSeries", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func metricArgs(f filter.Filters) []any {
	bucketStart, bucketEnd := chargs.BucketBounds(f.StartMs, f.EndMs)
	return []any{
		clickhouse.Named("teamID", uint32(f.TeamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("metricName", f.MetricName),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}
}
