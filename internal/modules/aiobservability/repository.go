package aiobservability

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const (
	metricGenAIClientOperationDuration = "gen_ai.client.operation.duration"
	metricGenAIClientTokenUsage        = "gen_ai.client.token.usage"
)

var llmOperations = []string{"chat", "completion", "text_completion", "generate_content", "embeddings"}

type Repository interface {
	QueryLLMRequestRateByModel(ctx context.Context, q QueryOptions) ([]modelRateRow, error)
	QueryLLMLatencyByModel(ctx context.Context, q QueryOptions) ([]modelLatencyRow, error)
	QueryLLMErrorRateByModel(ctx context.Context, q QueryOptions) ([]modelErrorRateRow, error)
	QueryLLMTokenUsageByModel(ctx context.Context, q QueryOptions) ([]tokenUsageRow, error)
	QueryLLMTokenUsageByProvider(ctx context.Context, q QueryOptions) ([]tokenUsageRow, error)
	QueryLLMCostByModel(ctx context.Context, q QueryOptions) ([]costRow, error)
	QueryLLMCostByProvider(ctx context.Context, q QueryOptions) ([]costRow, error)
	QueryTopExpensiveCalls(ctx context.Context, q QueryOptions) ([]callScanRow, error)
	QueryTopSlowCalls(ctx context.Context, q QueryOptions) ([]callScanRow, error)

	QueryPromptUsageByPrompt(ctx context.Context, q QueryOptions) ([]promptUsageRow, error)
	QueryPromptUsageByVersion(ctx context.Context, q QueryOptions) ([]promptUsageRow, error)
	QueryPromptLatencyByVersion(ctx context.Context, q QueryOptions) ([]promptLatencyRow, error)
	QueryPromptTokenUsageByVersion(ctx context.Context, q QueryOptions) ([]promptTokenUsageRow, error)
	QueryPromptCostByVersion(ctx context.Context, q QueryOptions) ([]promptCostRow, error)
	QueryPromptTraces(ctx context.Context, q QueryOptions) ([]traceScanRow, error)

	QueryAgentRunsByAgent(ctx context.Context, q QueryOptions) ([]agentRunRow, error)
	QueryToolCallsByTool(ctx context.Context, q QueryOptions) ([]toolCallRow, error)
	QueryToolErrorsByTool(ctx context.Context, q QueryOptions) ([]toolErrorRow, error)
	QueryToolLatencyByTool(ctx context.Context, q QueryOptions) ([]toolLatencyRow, error)

	QueryRetrievalRequestRateByStore(ctx context.Context, q QueryOptions) ([]retrievalRateRow, error)
	QueryRetrievalLatencyByStore(ctx context.Context, q QueryOptions) ([]retrievalLatencyRow, error)
	QueryRetrievalErrorsByStore(ctx context.Context, q QueryOptions) ([]retrievalErrorRow, error)

	QueryTraces(ctx context.Context, q QueryOptions) ([]traceScanRow, error)
	QueryFacets(ctx context.Context, q QueryOptions) ([]facetRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

const (
	spanProviderExpr  = "if(attributes.'gen_ai.provider.name'::String != '', attributes.'gen_ai.provider.name'::String, 'unknown')"
	spanModelExpr     = "if(attributes.'gen_ai.response.model'::String != '', attributes.'gen_ai.response.model'::String, if(attributes.'gen_ai.request.model'::String != '', attributes.'gen_ai.request.model'::String, 'unknown'))"
	spanOperationExpr = "if(attributes.'gen_ai.operation.name'::String != '', attributes.'gen_ai.operation.name'::String, name)"

	metricProviderExpr  = "if(attributes.'gen_ai.provider.name'::String != '', attributes.'gen_ai.provider.name'::String, 'unknown')"
	metricModelExpr     = "if(attributes.'gen_ai.response.model'::String != '', attributes.'gen_ai.response.model'::String, if(attributes.'gen_ai.request.model'::String != '', attributes.'gen_ai.request.model'::String, 'unknown'))"
	metricOperationExpr = "if(attributes.'gen_ai.operation.name'::String != '', attributes.'gen_ai.operation.name'::String, 'unknown')"

	promptNameExpr    = "multiIf(JSONExtractString(attributes.'_dd.ml_obs.prompt_tracking'::String, 'name') != '', JSONExtractString(attributes.'_dd.ml_obs.prompt_tracking'::String, 'name'), attributes.'gen_ai.prompt.name'::String != '', attributes.'gen_ai.prompt.name'::String, attributes.'prompt.name'::String != '', attributes.'prompt.name'::String, '')"
	promptVersionExpr = "multiIf(JSONExtractString(attributes.'_dd.ml_obs.prompt_tracking'::String, 'version') != '', JSONExtractString(attributes.'_dd.ml_obs.prompt_tracking'::String, 'version'), attributes.'gen_ai.prompt.version'::String != '', attributes.'gen_ai.prompt.version'::String, attributes.'prompt.version'::String != '', attributes.'prompt.version'::String, '')"

	agentNameExpr  = "multiIf(attributes.'gen_ai.agent.name'::String != '', attributes.'gen_ai.agent.name'::String, attributes.'gen_ai.workflow.name'::String != '', attributes.'gen_ai.workflow.name'::String, name)"
	toolNameExpr   = "if(attributes.'gen_ai.tool.name'::String != '', attributes.'gen_ai.tool.name'::String, name)"
	toolTypeExpr   = "if(attributes.'gen_ai.tool.type'::String != '', attributes.'gen_ai.tool.type'::String, 'unknown')"
	dataSourceExpr = "multiIf(attributes.'gen_ai.data_source.id'::String != '', attributes.'gen_ai.data_source.id'::String, attributes.'db.system'::String != '', attributes.'db.system'::String, attributes.'server.address'::String != '', attributes.'server.address'::String, 'unknown')"

	errorTypeExpr = "multiIf(attributes.'error.type'::String != '', attributes.'error.type'::String, exception_type != '', exception_type, status_message != '', status_message, '')"

	inputTokensExpr  = "greatest(toUInt64OrZero(attributes.'gen_ai.usage.input_tokens'::String), toUInt64OrZero(attributes.'gen_ai.usage.prompt_tokens'::String), toUInt64OrZero(attributes.'llm.usage.prompt_tokens'::String))"
	outputTokensExpr = "greatest(toUInt64OrZero(attributes.'gen_ai.usage.output_tokens'::String), toUInt64OrZero(attributes.'gen_ai.usage.completion_tokens'::String), toUInt64OrZero(attributes.'llm.usage.completion_tokens'::String))"
	totalTokensExpr  = "greatest(toUInt64OrZero(attributes.'gen_ai.usage.total_tokens'::String), toUInt64OrZero(attributes.'llm.usage.total_tokens'::String), " + inputTokensExpr + " + " + outputTokensExpr + ")"

	inputCostExpr  = "greatest(toFloat64OrZero(attributes.'gen_ai.usage.input_cost'::String), toFloat64OrZero(attributes.'llm.usage.input_cost'::String), toFloat64OrZero(attributes.'metrics.estimated_input_cost'::String), toFloat64OrZero(attributes.'_dd.estimated_input_cost'::String))"
	outputCostExpr = "greatest(toFloat64OrZero(attributes.'gen_ai.usage.output_cost'::String), toFloat64OrZero(attributes.'llm.usage.output_cost'::String), toFloat64OrZero(attributes.'metrics.estimated_output_cost'::String), toFloat64OrZero(attributes.'_dd.estimated_output_cost'::String))"
	totalCostExpr  = "greatest(toFloat64OrZero(attributes.'gen_ai.usage.total_cost'::String), toFloat64OrZero(attributes.'llm.usage.total_cost'::String), toFloat64OrZero(attributes.'metrics.estimated_total_cost'::String), toFloat64OrZero(attributes.'_dd.estimated_total_cost'::String), " + inputCostExpr + " + " + outputCostExpr + ")"
)

func (r *ClickHouseRepository) QueryLLMRequestRateByModel(ctx context.Context, q QueryOptions) ([]modelRateRow, error) {
	where, args := metricFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + metricProviderExpr + ` AS provider,
		       ` + metricModelExpr + ` AS model,
		       ` + metricOperationExpr + ` AS operation,
		       sum(hist_count) AS requests,
		       sum(hist_count) / @bucketGrainSec AS rate
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String IN @llmOps` + where + `
		GROUP BY timestamp, provider, model, operation
		ORDER BY timestamp ASC, requests DESC`
	args = append(metricArgs(q, metricGenAIClientOperationDuration), args...)
	args = append(args, clickhouse.Named("llmOps", llmOperations))
	args = timebucket.WithBucketGrainSec(args, q.StartMs, q.EndMs)
	var rows []modelRateRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryLLMRequestRateByModel", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryLLMLatencyByModel(ctx context.Context, q QueryOptions) ([]modelLatencyRow, error) {
	where, args := metricFilterClauses(q.Filters)
	query := `
		SELECT timestamp,
		       provider,
		       model,
		       operation,
		       avg_ms,
		       qs[1] * 1000 AS p50_ms,
		       qs[2] * 1000 AS p95_ms,
		       qs[3] * 1000 AS p99_ms
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		           ` + metricProviderExpr + ` AS provider,
		           ` + metricModelExpr + ` AS model,
		           ` + metricOperationExpr + ` AS operation,
		           sum(hist_sum) / nullIf(sum(hist_count), 0) * 1000 AS avg_ms,
		           quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		    FROM observability.metrics_1m
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name = @metricName
		         AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String IN @llmOps` + where + `
		    GROUP BY timestamp, provider, model, operation
		)
		ORDER BY timestamp ASC, p95_ms DESC`
	args = append(metricArgs(q, metricGenAIClientOperationDuration), args...)
	args = append(args, clickhouse.Named("llmOps", llmOperations))
	var rows []modelLatencyRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryLLMLatencyByModel", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryLLMErrorRateByModel(ctx context.Context, q QueryOptions) ([]modelErrorRateRow, error) {
	where, args := metricFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + metricProviderExpr + ` AS provider,
		       ` + metricModelExpr + ` AS model,
		       ` + metricOperationExpr + ` AS operation,
		       sum(hist_count) AS requests,
		       sumIf(hist_count, attributes.'error.type'::String != '') AS errors,
		       errors / nullIf(requests, 0) AS error_rate
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String IN @llmOps` + where + `
		GROUP BY timestamp, provider, model, operation
		ORDER BY timestamp ASC, error_rate DESC`
	args = append(metricArgs(q, metricGenAIClientOperationDuration), args...)
	args = append(args, clickhouse.Named("llmOps", llmOperations))
	var rows []modelErrorRateRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryLLMErrorRateByModel", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryLLMTokenUsageByModel(ctx context.Context, q QueryOptions) ([]tokenUsageRow, error) {
	return r.queryTokenUsage(ctx, q, true, "aiobservability.QueryLLMTokenUsageByModel")
}

func (r *ClickHouseRepository) QueryLLMTokenUsageByProvider(ctx context.Context, q QueryOptions) ([]tokenUsageRow, error) {
	return r.queryTokenUsage(ctx, q, false, "aiobservability.QueryLLMTokenUsageByProvider")
}

func (r *ClickHouseRepository) queryTokenUsage(ctx context.Context, q QueryOptions, byModel bool, op string) ([]tokenUsageRow, error) {
	where, args := metricFilterClauses(q.Filters)
	modelSelect := "'' AS model, '' AS operation"
	modelGroup := ""
	if byModel {
		modelSelect = metricModelExpr + " AS model, " + metricOperationExpr + " AS operation"
		modelGroup = ", model, operation"
	}
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + metricProviderExpr + ` AS provider,
		       ` + modelSelect + `,
		       sumIf(hist_sum + val_sum, attributes.'gen_ai.token.type'::String = 'input')  AS input_tokens,
		       sumIf(hist_sum + val_sum, attributes.'gen_ai.token.type'::String = 'output') AS output_tokens,
		       input_tokens + output_tokens AS total_tokens
		FROM observability.metrics_1m
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		GROUP BY timestamp, provider` + modelGroup + `
		ORDER BY timestamp ASC, total_tokens DESC`
	args = append(metricArgs(q, metricGenAIClientTokenUsage), args...)
	var rows []tokenUsageRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryLLMCostByModel(ctx context.Context, q QueryOptions) ([]costRow, error) {
	return r.queryCost(ctx, q, true, false, "aiobservability.QueryLLMCostByModel")
}

func (r *ClickHouseRepository) QueryLLMCostByProvider(ctx context.Context, q QueryOptions) ([]costRow, error) {
	return r.queryCost(ctx, q, false, false, "aiobservability.QueryLLMCostByProvider")
}

func (r *ClickHouseRepository) QueryTopExpensiveCalls(ctx context.Context, q QueryOptions) ([]callScanRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := callSelectSQL + `
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		ORDER BY estimated_total_cost DESC, duration_ms DESC
		LIMIT @limit`
	args = append(spanArgsWithLimit(q), args...)
	var rows []callScanRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "aiobservability.QueryTopExpensiveCalls", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryTopSlowCalls(ctx context.Context, q QueryOptions) ([]callScanRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := callSelectSQL + `
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		ORDER BY duration_ms DESC
		LIMIT @limit`
	args = append(spanArgsWithLimit(q), args...)
	var rows []callScanRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "aiobservability.QueryTopSlowCalls", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryPromptUsageByPrompt(ctx context.Context, q QueryOptions) ([]promptUsageRow, error) {
	return r.queryPromptUsage(ctx, q, false, "aiobservability.QueryPromptUsageByPrompt")
}

func (r *ClickHouseRepository) QueryPromptUsageByVersion(ctx context.Context, q QueryOptions) ([]promptUsageRow, error) {
	return r.queryPromptUsage(ctx, q, true, "aiobservability.QueryPromptUsageByVersion")
}

func (r *ClickHouseRepository) queryPromptUsage(ctx context.Context, q QueryOptions, includeVersion bool, op string) ([]promptUsageRow, error) {
	where, args := spanFilterClauses(q.Filters)
	versionSelect := "'' AS prompt_version"
	versionGroup := ""
	if includeVersion {
		versionSelect = promptVersionExpr + " AS prompt_version"
		versionGroup = ", prompt_version"
	}
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + promptNameExpr + ` AS prompt_name,
		       ` + versionSelect + `,
		       count() AS calls
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE ` + promptNameExpr + ` != ''` + where + `
		GROUP BY timestamp, prompt_name` + versionGroup + `
		ORDER BY timestamp ASC, calls DESC`
	args = append(spanArgs(q), args...)
	var rows []promptUsageRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryPromptLatencyByVersion(ctx context.Context, q QueryOptions) ([]promptLatencyRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT timestamp,
		       prompt_name,
		       prompt_version,
		       avg_ms,
		       qs[1] AS p50_ms,
		       qs[2] AS p95_ms,
		       qs[3] AS p99_ms
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		           ` + promptNameExpr + ` AS prompt_name,
		           ` + promptVersionExpr + ` AS prompt_version,
		           avg(duration_nano / 1000000.0) AS avg_ms,
		           quantilesTiming(0.5, 0.95, 0.99)(duration_nano / 1000000.0) AS qs
		    FROM observability.spans
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND timestamp BETWEEN @start AND @end
		    WHERE ` + promptNameExpr + ` != ''` + where + `
		    GROUP BY timestamp, prompt_name, prompt_version
		)
		ORDER BY timestamp ASC, p95_ms DESC`
	args = append(spanArgs(q), args...)
	var rows []promptLatencyRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryPromptLatencyByVersion", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryPromptTokenUsageByVersion(ctx context.Context, q QueryOptions) ([]promptTokenUsageRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + promptNameExpr + ` AS prompt_name,
		       ` + promptVersionExpr + ` AS prompt_version,
		       sum(` + inputTokensExpr + `)  AS input_tokens,
		       sum(` + outputTokensExpr + `) AS output_tokens,
		       sum(` + totalTokensExpr + `)  AS total_tokens
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE ` + promptNameExpr + ` != ''` + where + `
		GROUP BY timestamp, prompt_name, prompt_version
		ORDER BY timestamp ASC, total_tokens DESC`
	args = append(spanArgs(q), args...)
	var rows []promptTokenUsageRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryPromptTokenUsageByVersion", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryPromptCostByVersion(ctx context.Context, q QueryOptions) ([]promptCostRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + promptNameExpr + ` AS prompt_name,
		       ` + promptVersionExpr + ` AS prompt_version,
		       sum(` + inputCostExpr + `)  AS estimated_input_cost,
		       sum(` + outputCostExpr + `) AS estimated_output_cost,
		       sum(` + totalCostExpr + `)  AS estimated_total_cost
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE ` + promptNameExpr + ` != ''` + where + `
		GROUP BY timestamp, prompt_name, prompt_version
		ORDER BY timestamp ASC, estimated_total_cost DESC`
	args = append(spanArgs(q), args...)
	var rows []promptCostRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryPromptCostByVersion", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryPromptTraces(ctx context.Context, q QueryOptions) ([]traceScanRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := traceSelectSQL + `
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE ` + promptNameExpr + ` != ''` + where + `
		ORDER BY timestamp DESC
		LIMIT @limit`
	args = append(spanArgsWithLimit(q), args...)
	var rows []traceScanRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "aiobservability.QueryPromptTraces", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryAgentRunsByAgent(ctx context.Context, q QueryOptions) ([]agentRunRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + agentNameExpr + ` AS agent_name,
		       ` + spanOperationExpr + ` AS operation,
		       count() AS runs,
		       countIf(has_error OR status_code_string = 'ERROR' OR attributes.'error.type'::String != '') AS errors
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String IN ('invoke_agent', 'invoke_workflow', 'create_agent')` + where + `
		GROUP BY timestamp, agent_name, operation
		ORDER BY timestamp ASC, runs DESC`
	args = append(spanArgs(q), args...)
	var rows []agentRunRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryAgentRunsByAgent", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryToolCallsByTool(ctx context.Context, q QueryOptions) ([]toolCallRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + toolNameExpr + ` AS tool_name,
		       ` + toolTypeExpr + ` AS tool_type,
		       count() AS calls
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String = 'execute_tool'` + where + `
		GROUP BY timestamp, tool_name, tool_type
		ORDER BY timestamp ASC, calls DESC`
	args = append(spanArgs(q), args...)
	var rows []toolCallRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryToolCallsByTool", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryToolErrorsByTool(ctx context.Context, q QueryOptions) ([]toolErrorRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + toolNameExpr + ` AS tool_name,
		       ` + errorTypeExpr + ` AS error_type,
		       count() AS errors
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String = 'execute_tool'
		  AND (has_error OR status_code_string = 'ERROR' OR attributes.'error.type'::String != '')` + where + `
		GROUP BY timestamp, tool_name, error_type
		ORDER BY timestamp ASC, errors DESC`
	args = append(spanArgs(q), args...)
	var rows []toolErrorRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryToolErrorsByTool", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryToolLatencyByTool(ctx context.Context, q QueryOptions) ([]toolLatencyRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT timestamp,
		       tool_name,
		       tool_type,
		       avg_ms,
		       qs[1] AS p50_ms,
		       qs[2] AS p95_ms,
		       qs[3] AS p99_ms
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		           ` + toolNameExpr + ` AS tool_name,
		           ` + toolTypeExpr + ` AS tool_type,
		           avg(duration_nano / 1000000.0) AS avg_ms,
		           quantilesTiming(0.5, 0.95, 0.99)(duration_nano / 1000000.0) AS qs
		    FROM observability.spans
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String = 'execute_tool'` + where + `
		    GROUP BY timestamp, tool_name, tool_type
		)
		ORDER BY timestamp ASC, p95_ms DESC`
	args = append(spanArgs(q), args...)
	var rows []toolLatencyRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryToolLatencyByTool", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryRetrievalRequestRateByStore(ctx context.Context, q QueryOptions) ([]retrievalRateRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + dataSourceExpr + ` AS data_source,
		       ` + spanProviderExpr + ` AS provider,
		       count() AS requests,
		       count() / @bucketGrainSec AS rate
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String = 'retrieval'` + where + `
		GROUP BY timestamp, data_source, provider
		ORDER BY timestamp ASC, requests DESC`
	args = append(spanArgs(q), args...)
	args = timebucket.WithBucketGrainSec(args, q.StartMs, q.EndMs)
	var rows []retrievalRateRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryRetrievalRequestRateByStore", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryRetrievalLatencyByStore(ctx context.Context, q QueryOptions) ([]retrievalLatencyRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT timestamp,
		       data_source,
		       provider,
		       avg_ms,
		       qs[1] AS p50_ms,
		       qs[2] AS p95_ms,
		       qs[3] AS p99_ms
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		           ` + dataSourceExpr + ` AS data_source,
		           ` + spanProviderExpr + ` AS provider,
		           avg(duration_nano / 1000000.0) AS avg_ms,
		           quantilesTiming(0.5, 0.95, 0.99)(duration_nano / 1000000.0) AS qs
		    FROM observability.spans
		    PREWHERE team_id = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String = 'retrieval'` + where + `
		    GROUP BY timestamp, data_source, provider
		)
		ORDER BY timestamp ASC, p95_ms DESC`
	args = append(spanArgs(q), args...)
	var rows []retrievalLatencyRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryRetrievalLatencyByStore", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryRetrievalErrorsByStore(ctx context.Context, q QueryOptions) ([]retrievalErrorRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + dataSourceExpr + ` AS data_source,
		       ` + spanProviderExpr + ` AS provider,
		       count() AS requests,
		       countIf(has_error OR status_code_string = 'ERROR' OR attributes.'error.type'::String != '') AS errors,
		       errors / nullIf(requests, 0) AS error_rate
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String = 'retrieval'` + where + `
		GROUP BY timestamp, data_source, provider
		ORDER BY timestamp ASC, error_rate DESC`
	args = append(spanArgs(q), args...)
	var rows []retrievalErrorRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "aiobservability.QueryRetrievalErrorsByStore", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryTraces(ctx context.Context, q QueryOptions) ([]traceScanRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := traceSelectSQL + `
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		ORDER BY timestamp DESC
		LIMIT @limit`
	args = append(spanArgsWithLimit(q), args...)
	var rows []traceScanRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "aiobservability.QueryTraces", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) QueryFacets(ctx context.Context, q QueryOptions) ([]facetRow, error) {
	where, args := spanFilterClauses(q.Filters)
	query := `
		SELECT dim, value, count
		FROM (
		    (SELECT 'provider' AS dim, ` + spanProviderExpr + ` AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'model' AS dim, ` + spanModelExpr + ` AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'operation' AS dim, ` + spanOperationExpr + ` AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'service' AS dim, service AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'environment' AS dim, environment AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String != ''` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'prompt_name' AS dim, ` + promptNameExpr + ` AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE ` + promptNameExpr + ` != ''` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'agent_name' AS dim, ` + agentNameExpr + ` AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String IN ('invoke_agent', 'invoke_workflow', 'create_agent')` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'tool_name' AS dim, ` + toolNameExpr + ` AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String = 'execute_tool'` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		    UNION ALL
		    (SELECT 'data_source' AS dim, ` + dataSourceExpr + ` AS value, count() AS count
		    FROM observability.spans
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		    WHERE attributes.'gen_ai.operation.name'::String = 'retrieval'` + where + `
		    GROUP BY value
		    ORDER BY count DESC
		    LIMIT 20)
		)
		WHERE value != ''
		ORDER BY dim ASC, count DESC`
	args = append(spanArgs(q), args...)
	var rows []facetRow
	err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "aiobservability.QueryFacets", &rows, query, args...)
	return rows, err
}

func (r *ClickHouseRepository) queryCost(ctx context.Context, q QueryOptions, byModel bool, byPrompt bool, op string) ([]costRow, error) {
	where, args := spanFilterClauses(q.Filters)
	modelSelect := "'' AS model, '' AS operation"
	modelGroup := ""
	if byModel {
		modelSelect = spanModelExpr + " AS model, " + spanOperationExpr + " AS operation"
		modelGroup = ", model, operation"
	}
	promptPredicate := "attributes.'gen_ai.operation.name'::String != ''"
	if byPrompt {
		promptPredicate = promptNameExpr + " != ''"
	}
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(q.EndMs-q.StartMs) + ` AS timestamp,
		       ` + spanProviderExpr + ` AS provider,
		       ` + modelSelect + `,
		       sum(` + inputCostExpr + `)  AS estimated_input_cost,
		       sum(` + outputCostExpr + `) AS estimated_output_cost,
		       sum(` + totalCostExpr + `)  AS estimated_total_cost
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE ` + promptPredicate + where + `
		GROUP BY timestamp, provider` + modelGroup + `
		ORDER BY timestamp ASC, estimated_total_cost DESC`
	args = append(spanArgs(q), args...)
	var rows []costRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, query, args...)
	return rows, err
}

const callSelectSQL = `
		SELECT timestamp,
		       trace_id,
		       span_id,
		       service,
		       environment,
		       ` + spanProviderExpr + ` AS provider,
		       ` + spanModelExpr + ` AS model,
		       ` + spanOperationExpr + ` AS operation,
		       name,
		       duration_nano / 1000000.0 AS duration_ms,
		       ` + inputTokensExpr + ` AS input_tokens,
		       ` + outputTokensExpr + ` AS output_tokens,
		       ` + totalTokensExpr + ` AS total_tokens,
		       ` + totalCostExpr + ` AS estimated_total_cost,
		       status_code_string AS status,
		       ` + errorTypeExpr + ` AS error_type`

const traceSelectSQL = `
		SELECT timestamp,
		       trace_id,
		       span_id,
		       service,
		       environment,
		       ` + spanProviderExpr + ` AS provider,
		       ` + spanModelExpr + ` AS model,
		       ` + spanOperationExpr + ` AS operation,
		       name,
		       ` + promptNameExpr + ` AS prompt_name,
		       ` + promptVersionExpr + ` AS prompt_version,
		       ` + agentNameExpr + ` AS agent_name,
		       ` + toolNameExpr + ` AS tool_name,
		       ` + dataSourceExpr + ` AS data_source,
		       duration_nano / 1000000.0 AS duration_ms,
		       ` + totalTokensExpr + ` AS total_tokens,
		       ` + totalCostExpr + ` AS estimated_total_cost,
		       status_code_string AS status,
		       ` + errorTypeExpr + ` AS error_type`

func metricArgs(q QueryOptions, metricName string) []any {
	bucketStart, bucketEnd := bucketBounds(q.StartMs, q.EndMs)
	return []any{
		clickhouse.Named("teamID", uint32(q.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(q.StartMs)),
		clickhouse.Named("end", time.UnixMilli(q.EndMs)),
		clickhouse.Named("metricName", metricName),
	}
}

func spanArgs(q QueryOptions) []any {
	bucketStart, bucketEnd := bucketBounds(q.StartMs, q.EndMs)
	return []any{
		clickhouse.Named("teamID", uint32(q.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(q.StartMs)),
		clickhouse.Named("end", time.UnixMilli(q.EndMs)),
	}
}

func spanArgsWithLimit(q QueryOptions) []any {
	return append(spanArgs(q), clickhouse.Named("limit", uint64(q.Limit))) //nolint:gosec // G115
}

func bucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func metricFilterClauses(f Filters) (string, []any) {
	where := ""
	var args []any
	if len(f.Providers) > 0 {
		where += " AND " + metricProviderExpr + " IN @providers"
		args = append(args, clickhouse.Named("providers", f.Providers))
	}
	if len(f.Models) > 0 {
		where += " AND " + metricModelExpr + " IN @models"
		args = append(args, clickhouse.Named("models", f.Models))
	}
	if len(f.Operations) > 0 {
		where += " AND " + metricOperationExpr + " IN @operations"
		args = append(args, clickhouse.Named("operations", f.Operations))
	}
	if len(f.Services) > 0 {
		where += " AND service IN @services"
		args = append(args, clickhouse.Named("services", f.Services))
	}
	if len(f.Environments) > 0 {
		where += " AND environment IN @environments"
		args = append(args, clickhouse.Named("environments", f.Environments))
	}
	return where, args
}

func spanFilterClauses(f Filters) (string, []any) {
	where := ""
	var args []any
	if len(f.Providers) > 0 {
		where += " AND " + spanProviderExpr + " IN @providers"
		args = append(args, clickhouse.Named("providers", f.Providers))
	}
	if len(f.Models) > 0 {
		where += " AND " + spanModelExpr + " IN @models"
		args = append(args, clickhouse.Named("models", f.Models))
	}
	if len(f.Operations) > 0 {
		where += " AND " + spanOperationExpr + " IN @operations"
		args = append(args, clickhouse.Named("operations", f.Operations))
	}
	if len(f.Services) > 0 {
		where += " AND service IN @services"
		args = append(args, clickhouse.Named("services", f.Services))
	}
	if len(f.Environments) > 0 {
		where += " AND environment IN @environments"
		args = append(args, clickhouse.Named("environments", f.Environments))
	}
	if len(f.PromptNames) > 0 {
		where += " AND " + promptNameExpr + " IN @promptNames"
		args = append(args, clickhouse.Named("promptNames", f.PromptNames))
	}
	if len(f.PromptVersions) > 0 {
		where += " AND " + promptVersionExpr + " IN @promptVersions"
		args = append(args, clickhouse.Named("promptVersions", f.PromptVersions))
	}
	if len(f.AgentNames) > 0 {
		where += " AND " + agentNameExpr + " IN @agentNames"
		args = append(args, clickhouse.Named("agentNames", f.AgentNames))
	}
	if len(f.ToolNames) > 0 {
		where += " AND " + toolNameExpr + " IN @toolNames"
		args = append(args, clickhouse.Named("toolNames", f.ToolNames))
	}
	if len(f.DataSources) > 0 {
		where += " AND " + dataSourceExpr + " IN @dataSources"
		args = append(args, clickhouse.Named("dataSources", f.DataSources))
	}
	return where, args
}
