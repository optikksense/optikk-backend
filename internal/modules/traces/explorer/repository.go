package explorer

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

const traceIndexColumns = `trace_id,
		timestamp                                                  AS start_time,
		timestamp                                                  AS end_time,
		duration_nano                                              AS duration_ns,
		service                                                    AS root_service,
		name                                                       AS root_operation,
		status_code_string                                         AS root_status,
		http_method                                                AS root_http_method,
		response_status_code                                       AS root_http_status,
		1                                                          AS span_count,
		has_error,
		(CASE WHEN has_error THEN 1 ELSE 0 END)                    AS error_count,
		[service]                                                  AS service_set,
		false                                                      AS truncated,
		timestamp                                                  AS last_seen`

func (r *Repository) Query(ctx context.Context, req QueryRequest) ([]traceIndexRowDTO, bool, error) {
	resourceWhere, where, args := filter.BuildClauses(req.Filters)
	cur, _ := DecodeCursor(req.Cursor)
	if cur.TraceID != "" {
		where += ` AND (timestamp, trace_id) < (@curStart, @curTraceID)`
		args = append(args,
			clickhouse.Named("curStart", time.UnixMilli(int64(cur.StartMs))),
			clickhouse.Named("curTraceID", cur.TraceID),
		)
	}
	args = append(args, clickhouse.Named("pgLimit", uint64(req.Limit+1)))

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT ` + traceIndexColumns + `
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where + `
		ORDER BY timestamp DESC, trace_id DESC
		LIMIT @pgLimit`

	var rows []traceIndexRowDTO
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "traces.Query", &rows, query, args...); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > req.Limit
	if hasMore {
		rows = rows[:req.Limit]
	}
	return rows, hasMore, nil
}

func (r *Repository) QueryFacets(ctx context.Context, req FacetsRequest) (Facets, error) {
	if timebucket.UseHourRollup(req.EndTime - req.StartTime) {
		req.Filters.StartMs = timebucket.FloorMsToHour(req.Filters.StartMs)
	}
	resourceWhere, where, args := filter.BuildClauses(req.Filters)

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT topK(20)(service)              AS top_services,
		       topK(20)(name)                 AS top_operations,
		       topK(10)(http_method)          AS top_http_methods,
		       topK(15)(response_status_code) AS top_http_statuses,
		       topK(5)(status_code_string)    AS top_statuses
		FROM ` + timebucket.SpansRollup(req.EndTime-req.StartTime) + `
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where

	var rows []topKRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "facets.QueryFacets", &rows, query, args...); err != nil {
		return Facets{}, err
	}
	if len(rows) == 0 {
		return Facets{}, nil
	}
	return pivotTopK(rows[0]), nil
}

func pivotTopK(row topKRow) Facets {
	toFacetBuckets := func(vals []string) []FacetBucket {
		out := make([]FacetBucket, 0, len(vals))
		for _, v := range vals {
			if v != "" {
				out = append(out, FacetBucket{Value: v})
			}
		}
		return out
	}
	return Facets{
		Service:    toFacetBuckets(row.TopServices),
		Operation:  toFacetBuckets(row.TopOperations),
		HTTPMethod: toFacetBuckets(row.TopHTTPMethods),
		HTTPStatus: toFacetBuckets(row.TopHTTPStatuses),
		Status:     toFacetBuckets(row.TopStatuses),
	}
}

func (r *Repository) QueryTrend(ctx context.Context, req TrendRequest) ([]TrendBucket, error) {
	if timebucket.UseHourRollup(req.EndTime - req.StartTime) {
		req.Filters.StartMs = timebucket.FloorMsToHour(req.Filters.StartMs)
	}
	resourceWhere, where, args := filter.BuildClauses(req.Filters)
	grainSQL := timebucket.DisplayGrainSQL(req.EndTime - req.StartTime)

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT ` + grainSQL + `                          AS time_bucket,
		       sum(request_count) - sum(error_count)     AS total,
		       sum(error_count)                          AS errors
		FROM ` + timebucket.SpansRollup(req.EndTime-req.StartTime) + `
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end AND is_root = 1` + where + `
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`

	var rows []struct {
		TimeBucket time.Time `ch:"time_bucket"`
		Total      int64     `ch:"total"`
		Errors     uint64    `ch:"errors"`
	}
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trend.QueryTrend", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]TrendBucket, len(rows))
	for i, r := range rows {
		total := r.Total
		if total < 0 {
			total = 0
		}
		out[i] = TrendBucket{
			TimeBucket: timebucket.FormatDisplayBucket(r.TimeBucket),
			Total:      uint64(total),
			Errors:     r.Errors,
		}
	}
	return out, nil
}

func (r *Repository) SuggestScalar(ctx context.Context, teamID, startMs, endMs int64, field, prefix string, limit int) ([]Suggestion, error) {
	if timebucket.UseHourRollup(endMs - startMs) {
		startMs = timebucket.FloorMsToHour(startMs)
	}
	column := scalarFieldExpr(field)
	query := `
		SELECT ` + column + `        AS value,
		       sum(request_count)    AS count
		FROM ` + timebucket.SpansRollup(endMs-startMs) + `
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @startMs AND @endMs
		  AND ` + column + ` != ''
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit`
	var rows []suggestionRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "suggest.SuggestScalar", &rows, query, suggestArgs(teamID, startMs, endMs, prefix, limit)...); err != nil {
		return nil, err
	}
	out := make([]Suggestion, len(rows))
	for i, row := range rows {
		out[i] = Suggestion{Value: row.Value, Count: row.Count}
	}
	return out, nil
}

func (r *Repository) SuggestAttribute(ctx context.Context, teamID, startMs, endMs int64, attrKey, prefix string, limit int) ([]Suggestion, error) {
	const query = `
		SELECT attributes[@attrKey]::String AS value, count() AS count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		WHERE timestamp BETWEEN @startMs AND @endMs
		  AND value != ''
		  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
		GROUP BY value
		ORDER BY count DESC
		LIMIT @limit`
	args := append(suggestArgs(teamID, startMs, endMs, prefix, limit),
		clickhouse.Named("attrKey", strings.TrimPrefix(attrKey, "@")),
	)
	var rows []suggestionRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "suggest.SuggestAttribute", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]Suggestion, len(rows))
	for i, row := range rows {
		out[i] = Suggestion{Value: row.Value, Count: row.Count}
	}
	return out, nil
}

func suggestArgs(teamID, startMs, endMs int64, prefix string, limit int) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", timebucket.BucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.BucketStart(endMs/1000)+uint32(timebucket.BucketSeconds)),
		clickhouse.Named("startMs", time.UnixMilli(startMs)),
		clickhouse.Named("endMs", time.UnixMilli(endMs)),
		clickhouse.Named("prefix", prefix),
		clickhouse.Named("limit", uint64(limit)),
	}
}

func scalarFieldExpr(field string) string {
	switch field {
	case "service":
		return "service"
	case "operation":
		return "name"
	case "http_method":
		return "http_method"
	case "http_status":
		return "response_status_code"
	case "status":
		return "status_code_string"
	case "environment":
		return "environment"
	default:
		return "''"
	}
}
