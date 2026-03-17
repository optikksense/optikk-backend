package dashboard

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type QueryBuilder interface {
	Build() (string, []any)
}

type BaseQueryBuilder struct {
	teamID    int64
	startMs   int64
	endMs     int64
	modelName string
	limit     int
}

func NewBaseQueryBuilder(teamID int64, startMs, endMs int64) *BaseQueryBuilder {
	return &BaseQueryBuilder{
		teamID:  teamID,
		startMs: startMs,
		endMs:   endMs,
		limit:   100,
	}
}

func (b *BaseQueryBuilder) WithModelName(modelName string) *BaseQueryBuilder {
	b.modelName = modelName
	return b
}

func (b *BaseQueryBuilder) WithLimit(limit int) *BaseQueryBuilder {
	b.limit = limit
	return b
}

const tableMetrics = "observability.metrics"

func attrStr(key string) string { return "attributes.'" + key + "'::String" }
func attrInt(key string) string { return "attributes.'" + key + "'::Int64" }
func attrFlt(key string) string { return "attributes.'" + key + "'::Float64" }

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (b *BaseQueryBuilder) baseWhereClause() (string, []any) {
	modelExpr := attrStr("gen.ai.request.model")
	where := fmt.Sprintf("WHERE team_id = @teamID AND timestamp BETWEEN @start AND @end AND %s <> ''", modelExpr)
	args := []any{
		clickhouse.Named("teamID", uint32(b.teamID)),
		clickhouse.Named("start", time.UnixMilli(b.startMs)),
		clickhouse.Named("end", time.UnixMilli(b.endMs)),
	}

	if b.modelName != "" {
		where += fmt.Sprintf(" AND %s = @modelName", modelExpr)
		args = append(args, clickhouse.Named("modelName", b.modelName))
	}

	return where, args
}

// --- Summary ---

type SummaryQueryBuilder struct{ *BaseQueryBuilder }

func NewSummaryQueryBuilder(teamID int64, startMs, endMs int64) *SummaryQueryBuilder {
	return &SummaryQueryBuilder{BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs)}
}

func (b *SummaryQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	durationMs := attrFlt("duration_ms")
	inputTokens := attrInt("gen.ai.usage.input_tokens")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	costUSD := attrFlt("ai.cost_usd")
	cacheHit := attrInt("ai.cache_hit")
	aiTimeout := attrInt("ai.timeout")
	piiDetected := attrInt("ai.security.pii_detected")
	guardrailBlocked := attrInt("ai.security.guardrail_blocked")

	query := fmt.Sprintf(`
		SELECT COUNT(*) as total_requests,
		       COUNT(*) / GREATEST(dateDiff('second', @start, @end), 1) as avg_qps,
		       AVG(%s) as avg_latency_ms,
		       AVG(%s) as p95_latency_ms,
		       SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as timeout_count,
		       countIf(has_error) as error_count,
		       SUM(COALESCE(%s, 0) + COALESCE(%s, 0)) as total_tokens,
		       SUM(COALESCE(%s, 0)) as total_cost_usd,
		       AVG(COALESCE(%s, 0)) as avg_cost_per_query,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as cache_hit_rate,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as pii_detection_rate,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as guardrail_block_rate,
		       AVG(CASE WHEN %s > 0 THEN COALESCE(%s, 0) / (%s / 1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       COUNT(DISTINCT %s) as active_models
		FROM %s
		%s
	`,
		durationMs, durationMs,
		aiTimeout,
		inputTokens, outputTokens,
		costUSD, costUSD,
		cacheHit, piiDetected, guardrailBlocked,
		durationMs, outputTokens, durationMs,
		attrStr("gen.ai.request.model"),
		tableMetrics, where)

	return query, args
}

// --- Model List ---

type ModelListQueryBuilder struct{ *BaseQueryBuilder }

func NewModelListQueryBuilder(teamID int64, startMs, endMs int64) *ModelListQueryBuilder {
	return &ModelListQueryBuilder{BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs)}
}

func (b *ModelListQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()
	query := fmt.Sprintf(`
		SELECT %s as model_name, %s as model_provider
		FROM %s %s
		GROUP BY model_name, model_provider ORDER BY model_name ASC LIMIT %d
	`, attrStr("gen.ai.request.model"), attrStr("server.address"), tableMetrics, where, b.limit)
	return query, args
}

// --- Time Series ---

type TimeSeriesQueryBuilder struct {
	*BaseQueryBuilder
	bucketStrategy timebucket.Strategy
	selectFields   []string
	groupByFields  []string
	orderBy        string
}

func NewTimeSeriesQueryBuilder(teamID int64, startMs, endMs int64, strategy timebucket.Strategy) *TimeSeriesQueryBuilder {
	return &TimeSeriesQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs),
		bucketStrategy:   strategy,
		orderBy:          "time_bucket ASC, model_name ASC",
	}
}

func (b *TimeSeriesQueryBuilder) WithSelectFields(fields []string) *TimeSeriesQueryBuilder {
	b.selectFields = fields
	return b
}

func (b *TimeSeriesQueryBuilder) WithGroupByFields(fields []string) *TimeSeriesQueryBuilder {
	b.groupByFields = fields
	return b
}

func (b *TimeSeriesQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()
	bucketExpr := b.bucketStrategy.GetBucketExpression()
	selectClause := strings.Join(b.selectFields, ",\n       ")
	groupByClause := "model_name, " + bucketExpr
	if len(b.groupByFields) > 0 {
		groupByClause = strings.Join(append([]string{"model_name", bucketExpr}, b.groupByFields...), ", ")
	}
	query := fmt.Sprintf(`
		SELECT %s as model_name, %s as time_bucket, %s
		FROM %s %s
		GROUP BY %s ORDER BY %s
	`, attrStr("gen.ai.request.model"), bucketExpr, selectClause, tableMetrics, where, groupByClause, b.orderBy)
	return query, args
}

// --- Latency Histogram ---

type LatencyHistogramQueryBuilder struct {
	*BaseQueryBuilder
	bucketSizeMs int64
}

func NewLatencyHistogramQueryBuilder(teamID int64, startMs, endMs int64) *LatencyHistogramQueryBuilder {
	return &LatencyHistogramQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs).WithLimit(200),
		bucketSizeMs:     100,
	}
}

func (b *LatencyHistogramQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()
	durationMs := attrFlt("duration_ms")
	query := fmt.Sprintf(`
		SELECT %s as model_name, FLOOR(%s / %d) * %d as bucket_ms, COUNT(*) as request_count
		FROM %s %s
		GROUP BY model_name, bucket_ms ORDER BY model_name, bucket_ms ASC LIMIT %d
	`, attrStr("gen.ai.request.model"), durationMs, b.bucketSizeMs, b.bucketSizeMs, tableMetrics, where, b.limit)
	return query, args
}
